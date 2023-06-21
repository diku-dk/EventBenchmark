using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Client.Infra;
using Common.Distribution;
using Common.Distribution.YCSB;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Confluent.Kafka;
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

        private StreamSubscriptionHandle<CustomerStatusUpdate> customerWorkerSubscription;
        private StreamSubscriptionHandle<int> sellerWorkerSubscription;
        private StreamSubscriptionHandle<int> deliveryWorkerSubscription;

        private readonly CustomerWorkerConfig customerWorkerConfig;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int concurrencyLevel;

        private readonly Random random;

        private readonly ILogger logger;

        public WorkloadEmitter(IClusterClient clusterClient,
                                CustomerWorkerConfig customerWorkerConfig,
                                DistributionType customerDistribution,
                                Interval customerRange,
                                int concurrencyLevel) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.customerWorkerConfig = customerWorkerConfig;
            this.concurrencyLevel = concurrencyLevel;

            this.random = new Random();

            NumberGenerator sellerIdGenerator = customerWorkerConfig.sellerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(customerWorkerConfig.sellerRange.max * 0.3), customerWorkerConfig.sellerRange.min, customerWorkerConfig.sellerRange.max) :
                                customerDistribution == DistributionType.UNIFORM ?
                                new UniformLongGenerator(customerWorkerConfig.sellerRange.min, customerWorkerConfig.sellerRange.max) :
                                new ZipfianGenerator(customerWorkerConfig.sellerRange.min, customerWorkerConfig.sellerRange.max);

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
            SetUpDeliveryWorkerListener();

            int submitted = 0;
            while (submitted < concurrencyLevel)
            {
                SubmitTransaction();
                submitted++;
            }
            // signal the queue has been drained
            Shared.WaitHandle.Set();

            while (IsRunning())
            {
                logger.LogInformation("[WorkloadEmitter] Will wait for result at {0}", DateTime.Now.Millisecond);
                // wait for results
                Shared.ResultQueue.Take();
                logger.LogInformation("[WorkloadEmitter] Received a result at {0}. Submitting a new transaction.", DateTime.Now.Millisecond);

                SubmitTransaction();
                submitted++;

                if(submitted % concurrencyLevel == 0)
                {
                    Shared.WaitHandle.Set();
                }

            }

            await customerWorkerSubscription.UnsubscribeAsync();
            await sellerWorkerSubscription.UnsubscribeAsync();
            await deliveryWorkerSubscription.UnsubscribeAsync();
        }

        private async void SubmitTransaction()
        {
            long threadId = System.Environment.CurrentManagedThreadId;
            try
            {
                this.logger.LogWarning("Thread ID {0} Submit transaction called", threadId);

                TransactionIdentifier txId = Shared.Workload.Take();

                this.logger.LogWarning("Thread ID {0} Transaction type {0}", threadId, txId.type.ToString());

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

                        // init customer dynamically
                        if (this.customerStatusCache.ContainsKey(grainID))
                        {
                            customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(grainID);
                        }
                        else
                        {
                            customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(grainID);
                            await customerWorker.Init(this.customerWorkerConfig);
                        }

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
                        var streamOutgoing = this.streamProvider.GetStream<TransactionIdentifier>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        return;
                    }
                    case TransactionType.PRICE_UPDATE:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionIdentifier>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        return;
                    }
                    // seller
                    case TransactionType.DELETE_PRODUCT:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionIdentifier>(StreamingConstants.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(txId);
                        return;
                    }
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, 0.ToString());
                        _ = streamOutgoing.OnNextAsync(txId.tid);
                        return;
                    }
                    default: { throw new Exception("Thread ID " + threadId + " Unknown transaction type defined!"); }
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }

            return;
        }

        private Task UpdateCustomerStatusAsync(CustomerStatusUpdate update, StreamSequenceToken token = null)
        {
            var old = this.customerStatusCache[update.customerId];
            this.logger.LogWarning("Attempt to update customer worker {0} status in cache. Previous {1} Update {2}",
                update.customerId, old, update.status);
            this.customerStatusCache[update.customerId] = update.status;

            if(update.status == CustomerWorkerStatus.CHECKOUT_NOT_SENT || update.status == CustomerWorkerStatus.CHECKOUT_FAILED)
            {
                // no event will be returned from the application
                Shared.ResultQueue.Add(null);
            }

            return Task.CompletedTask;
        }

        private async void SetUpCustomerWorkerListener()
        {
            IAsyncStream<CustomerStatusUpdate> resultStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
            this.customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        private Task AddToResultQueue(int num, StreamSequenceToken token = null)
        {
            Shared.ResultQueue.Add(null);
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