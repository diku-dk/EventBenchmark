using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Distribution;
using Common.Distribution.YCSB;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;

namespace Client.Workload
{

    public class WorkloadEmitter : Stoppable
	{

        private readonly IClusterClient orleansClient;

        private readonly IStreamProvider streamProvider;

        public readonly Dictionary<TransactionType, NumberGenerator> keyGeneratorPerWorkloadType;

        private StreamSubscriptionHandle<CustomerWorkerStatusUpdate> customerWorkerSubscription;

        private StreamSubscriptionHandle<int> sellerWorkerSubscription;

        private StreamSubscriptionHandle<int> deliveryWorkerSubscription;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int concurrencyLevel;

        private readonly int delayBetweenRequests;

        private readonly ILogger logger;

        public WorkloadEmitter(IClusterClient clusterClient,
                                DistributionType sellerDistribution,
                                Interval sellerRange,
                                DistributionType customerDistribution,
                                Interval customerRange,
                                int concurrencyLevel,
                                int delayBetweenRequests) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.concurrencyLevel = concurrencyLevel;
            this.delayBetweenRequests = delayBetweenRequests;

            NumberGenerator sellerIdGenerator = sellerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(sellerRange.max * 0.3), sellerRange.min, sellerRange.max) :
                                sellerDistribution == DistributionType.UNIFORM ?
                                new UniformLongGenerator(sellerRange.min, sellerRange.max) :
                                new ZipfianGenerator(sellerRange.min, sellerRange.max);

            NumberGenerator customerIdGenerator = customerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(customerRange.max * 0.3), customerRange.min, customerRange.max) :
                                customerDistribution == DistributionType.UNIFORM ?
                                    new UniformLongGenerator(customerRange.min, customerRange.max) :
                                    new ZipfianGenerator(customerRange.min, customerRange.max);

            this.keyGeneratorPerWorkloadType = new()
            {
                [TransactionType.PRICE_UPDATE] = sellerIdGenerator,
                [TransactionType.DELETE_PRODUCT] = sellerIdGenerator,
                [TransactionType.CUSTOMER_SESSION] = customerIdGenerator
            };

            this.customerStatusCache = new();

            this.logger = LoggerProxy.GetInstance("WorkloadEmitter");
        }

		public async Task<(DateTime startTime, DateTime finishTime)> Run()
		{

            SetUpCustomerWorkerListener();
            SetUpSellerWorkerListener();
            SetUpDeliveryWorkerListener();

            DateTime startTime = DateTime.Now;

            int submitted = 0;
            while (submitted < concurrencyLevel)
            {
                SubmitTransaction();
                submitted++;
            }

            // signal the queue has been drained
            Shared.WaitHandle.Set();

            logger.LogInformation("[WorkloadEmitter] Will start waiting for result at {0}", DateTime.Now);

            while (IsRunning())
            {
                // wait for results
                Shared.ResultQueue.Take();

                logger.LogInformation("[WorkloadEmitter] Received a result at {0}. Submitting a new transaction then", DateTime.Now);

                SubmitTransaction();
                submitted++;

                if(submitted % concurrencyLevel == 0)
                {
                    Shared.WaitHandle.Set();
                }

                // throttle
                if (this.delayBetweenRequests > 0)
                {
                    await Task.Delay(this.delayBetweenRequests);
                }

            }

            DateTime finishTime = DateTime.Now;

            await Task.WhenAll(
             customerWorkerSubscription.UnsubscribeAsync(),
             sellerWorkerSubscription.UnsubscribeAsync(),
             deliveryWorkerSubscription.UnsubscribeAsync());

            return (startTime, finishTime);
        }

        private void SubmitTransaction()
        {
            long threadId = Environment.CurrentManagedThreadId;
            TransactionInput txId = Shared.Workload.Take();
            try
            {
                long grainID;

                switch (txId.type)
                {
                    //customer worker
                    case TransactionType.CUSTOMER_SESSION:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        // but make sure there is no active session for the customer. if so, pick another customer
                        while (this.customerStatusCache.ContainsKey(grainID) &&
                                customerStatusCache[grainID] == CustomerWorkerStatus.BROWSING)
                        {
                            grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        }
                        
                        this.logger.LogInformation("Customer worker {0} defined!", grainID);
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.CustomerStreamId, grainID.ToString());
                        this.customerStatusCache[grainID] = CustomerWorkerStatus.BROWSING;
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        this.logger.LogInformation("Customer worker {0} message sent!", grainID);
                        break;
                    }
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, "0");
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        break;
                    }
                    // seller worker
                    case TransactionType.DASHBOARD:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    case TransactionType.PRICE_UPDATE:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    // seller
                    case TransactionType.DELETE_PRODUCT:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    default:
                    {
                        this.logger.LogError("Thread ID " + threadId + " Unknown transaction type defined!");
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }

        }

        private Task UpdateCustomerStatusAsync(CustomerWorkerStatusUpdate update, StreamSequenceToken token = null)
        {
            if (update.result)
            {
                Shared.ResultQueue.Add(0);
                return Task.CompletedTask;
            }
            var old = this.customerStatusCache[update.customerId];
            this.logger.LogInformation("Attempt to update customer worker {0} status in cache. Previous {1} Update {2}",
                update.customerId, old, update.status);
            this.customerStatusCache[update.customerId] = update.status;
            return Task.CompletedTask;
        }

        private async void SetUpCustomerWorkerListener()
        {
            IAsyncStream<CustomerWorkerStatusUpdate> resultStream = streamProvider.GetStream<CustomerWorkerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        private Task AddToResultQueue(int tid, StreamSequenceToken token = null)
        {
            Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private async void SetUpSellerWorkerListener()
        {
            IAsyncStream<int> resultStream = streamProvider.GetStream<int>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.sellerWorkerSubscription = await resultStream.SubscribeAsync(AddToResultQueue);
        }

        private async void SetUpDeliveryWorkerListener()
        {
            IAsyncStream<int> resultStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.deliveryWorkerSubscription = await resultStream.SubscribeAsync(AddToResultQueue);
        }

    }
}