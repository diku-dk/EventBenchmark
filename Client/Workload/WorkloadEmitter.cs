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
using GrainInterfaces.Workers;
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

        private StreamSubscriptionHandle<SellerWorkerStatusUpdate> sellerWorkerSubscription;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int concurrencyLevel;

        private readonly Interval deliveryRange;

        private readonly ILogger logger;

        public WorkloadEmitter(IClusterClient clusterClient,
                                DistributionType sellerDistribution,
                                Interval sellerRange,
                                DistributionType customerDistribution,
                                Interval customerRange,
                                Interval deliveryRange,
                                int concurrencyLevel) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.concurrencyLevel = concurrencyLevel;
            this.deliveryRange = deliveryRange;

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

		public async void Run()
		{

            SetUpCustomerWorkerListener();
            SetUpSellerWorkerListener();

            int submitted = 0;
            while (submitted < concurrencyLevel)
            {
                SubmitTransaction();
                submitted++;
            }

            // signal the queue has been drained
            Shared.WaitHandle.Set();

            logger.LogInformation("[WorkloadEmitter] Will start waiting for result at {0}", DateTime.Now.Millisecond);

            while (IsRunning())
            {
              
                // wait for results
                Shared.ResultQueue.Take();

                logger.LogInformation("[WorkloadEmitter] Received a result at {0}. Submitting a new transaction at", DateTime.Now.Millisecond);

                SubmitTransaction();
                submitted++;

                if(submitted % concurrencyLevel == 0)
                {
                    Shared.WaitHandle.Set();
                }

                await Task.Delay(10000);

            }

            await customerWorkerSubscription.UnsubscribeAsync();
            await sellerWorkerSubscription.UnsubscribeAsync();
        }

        private void SubmitTransaction()
        {
            long threadId = System.Environment.CurrentManagedThreadId;
            this.logger.LogWarning("Thread ID {0} Submit transaction called", threadId);
            TransactionInput txId = Shared.Workload.Take();
            this.logger.LogWarning("Thread ID {0} Transaction type {0}", threadId, txId.type.ToString());
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
                        ICustomerWorker customerWorker;
                        while (this.customerStatusCache.ContainsKey(grainID) &&
                                customerStatusCache[grainID] == CustomerWorkerStatus.BROWSING)
                        {
                            grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        }
                        customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(grainID);
                        
                        this.logger.LogWarning("Customer worker {0} defined!", grainID);
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.CustomerStreamId, grainID.ToString());
                        this.customerStatusCache[grainID] = CustomerWorkerStatus.BROWSING;
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        this.logger.LogWarning("Customer worker {0} message sent!", grainID);
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
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        // distribute workload accross many actors
                        int workerId = new Random().Next(deliveryRange.min, deliveryRange.max + 1);
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, workerId.ToString());
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        break;
                    }
                    default: { throw new Exception("Thread ID " + threadId + " Unknown transaction type defined!"); }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }

        }

        private Task UpdateCustomerStatusAsync(CustomerWorkerStatusUpdate update, StreamSequenceToken token = null)
        {
            var old = this.customerStatusCache[update.customerId];
            this.logger.LogWarning("Attempt to update customer worker {0} status in cache. Previous {1} Update {2}",
                update.customerId, old, update.status);
            this.customerStatusCache[update.customerId] = update.status;
            Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private async void SetUpCustomerWorkerListener()
        {
            IAsyncStream<CustomerWorkerStatusUpdate> resultStream = streamProvider.GetStream<CustomerWorkerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        private Task UpdateSellerStatusAsync(SellerWorkerStatusUpdate update, StreamSequenceToken token = null)
        {
            Shared.ResultQueue.Add(0);
            return Task.CompletedTask;
        }

        private async void SetUpSellerWorkerListener()
        {
            IAsyncStream<SellerWorkerStatusUpdate> resultStream = streamProvider.GetStream<SellerWorkerStatusUpdate>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.sellerWorkerSubscription = await resultStream.SubscribeAsync(UpdateSellerStatusAsync);
        }

    }
}