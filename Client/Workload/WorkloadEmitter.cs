using System.Collections.Concurrent;
using System.Diagnostics;
using Common.Distribution;
using Common.Distribution.YCSB;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Runtime;
using System;

namespace Client.Workload
{

    public class WorkloadEmitter
	{

        private readonly IClusterClient orleansClient;

        private readonly IDictionary<TransactionType, int> transactionDistribution;
        private readonly Random random;

        private readonly IStreamProvider streamProvider;

        public readonly Dictionary<TransactionType, NumberGenerator> keyGeneratorPerWorkloadType;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int executionTime;
        private readonly int concurrencyLevel;

        private readonly int delayBetweenRequests;

        private readonly ILogger logger;

        public WorkloadEmitter(IClusterClient clusterClient,
                                IDictionary<TransactionType,int> transactionDistribution,
                                DistributionType sellerDistribution,
                                Interval sellerRange,
                                DistributionType customerDistribution,
                                Interval customerRange,
                                int concurrencyLevel,
                                int executionTime,
                                int delayBetweenRequests) : base()
        {
            this.orleansClient = clusterClient;
            this.transactionDistribution = transactionDistribution;
            this.random = new Random();
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            this.concurrencyLevel = concurrencyLevel;
            this.executionTime = executionTime;
            this.delayBetweenRequests = delayBetweenRequests;

            NumberGenerator sellerIdGenerator = sellerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(sellerRange.max * 0.3), sellerRange.min, sellerRange.max) :
                                sellerDistribution == DistributionType.UNIFORM ?
                                new UniformLongGenerator(sellerRange.min, sellerRange.max) :
                                new ZipfianGenerator(sellerRange.min, sellerRange.max);

            NumberGenerator customerIdGenerator = customerDistribution ==
                                DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(customerRange.max * 0.5), customerRange.min, customerRange.max) :
                                customerDistribution == DistributionType.UNIFORM ?
                                    new UniformLongGenerator(customerRange.min, customerRange.max) :
                                    new ZipfianGenerator(customerRange.min, customerRange.max);

            this.keyGeneratorPerWorkloadType = new()
            {
                [TransactionType.PRICE_UPDATE] = sellerIdGenerator,
                [TransactionType.DELETE_PRODUCT] = sellerIdGenerator,
                [TransactionType.DASHBOARD] = sellerIdGenerator,
                [TransactionType.CUSTOMER_SESSION] = customerIdGenerator
            };

            this.customerStatusCache = new();

            this.logger = LoggerProxy.GetInstance("WorkloadEmitter");
        }

		public async Task<(DateTime startTime, DateTime finishTime)> Run()
		{
            int currentTid = 1;
            logger.LogInformation("[Workload emitter] Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

            Stopwatch s = new Stopwatch();
            s.Start();
            var startTime = DateTime.UtcNow;

            List<Task> tasks = new(concurrencyLevel+1);

            while (currentTid <= concurrencyLevel)
            {
                TransactionType tx = PickTransactionFromDistribution();
                var txId = new TransactionInput(currentTid, tx);

                if (txId.type == TransactionType.CUSTOMER_SESSION)
                {
                    tasks.Add( Task.Run(async () => await SubmitCustomerSession(txId)) );
                }else
                {
                    tasks.Add(Task.Run(() => SubmitNonCustomerTransaction(txId)));
                }

                currentTid++;
            }

            logger.LogInformation("[Workload emitter] Started waiting for results to send remaining transactions...");
            var execTime = TimeSpan.FromMilliseconds(executionTime);
            while (s.Elapsed < execTime)
            {
                // wait for results
                // logger.LogInformation("[Workload emitter] Retrieving a result...");

                Task taskRes = await Task.WhenAny(tasks);
                tasks.Remove(taskRes);

                // logger.LogInformation("[Workload emitter] Retrieving a transaction...");
                TransactionType tx = PickTransactionFromDistribution();
                var txId = new TransactionInput(currentTid, tx);

                if (txId.type == TransactionType.CUSTOMER_SESSION)
                {
                    tasks.Add(Task.Run(async () => await SubmitCustomerSession(txId)));
                }
                else
                {
                    tasks.Add(Task.Run(() => SubmitNonCustomerTransaction(txId)));
                }

                currentTid++;

                // logger.LogInformation("[Workload emitter] Transaction submitted.");

                //if (Shared.Workload.Count < concurrencyLevel)
                //{
                //    // ideally we should never enter here
                //    logger.LogWarning("[Workload emitter] Requesting more transactions to workload generator...");
                //    Shared.WaitHandle.Add(0);
                //}

                // throttle
                if (this.delayBetweenRequests > 0)
                {
                    await Task.Delay(this.delayBetweenRequests);
                }
                
            }

            var finishTime = DateTime.UtcNow;
            s.Stop();

            logger.LogInformation("[Workload emitter] Finished at {0}. Last TID submitted was {1}", finishTime, currentTid);

            return (startTime, finishTime);
        }

        private TransactionType PickTransactionFromDistribution()
        {
            int x = this.random.Next(0, 101);
            foreach (var entry in transactionDistribution)
            {
                if (x <= entry.Value)
                {
                    return entry.Key;
                }
            }
            return TransactionType.NONE;
        }

        private async Task SubmitCustomerSession(TransactionInput txId)
        {
            long grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
            // but make sure there is no active session for the customer. if so, pick another customer
            int count = 1;
            while ((this.customerStatusCache.ContainsKey(grainID) &&
                    !customerStatusCache.TryUpdate(grainID, CustomerWorkerStatus.BROWSING, CustomerWorkerStatus.IDLE)) ||
                    !customerStatusCache.TryAdd(grainID, CustomerWorkerStatus.BROWSING))
            {
                if (count == 10)
                {
                    logger.LogWarning("[Workload emitter] Could not find an available customer! Perhaps should increase the number of customer next time?");
                    return;
                }
                grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                count++;
            }

            var streamId = StreamId.Create(StreamingConstants.CustomerWorkerNameSpace, grainID.ToString());
            var streamOutgoing = this.streamProvider.GetStream<int>(streamId);

            await streamOutgoing.OnNextAsync(txId.tid);
            this.customerStatusCache[grainID] = CustomerWorkerStatus.BROWSING;
        }

        private Task SubmitNonCustomerTransaction(TransactionInput txId)
        {
            // this.logger.LogInformation("Sending a new {0} transaction with ID {1}", txId.type, txId.tid);
            try
            {
                switch (txId.type)
                {
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryWorkerNameSpace, "0");
                        return streamOutgoing.OnNextAsync(txId.tid);
                    }
                    // seller worker
                    case TransactionType.DASHBOARD:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainID.ToString());
                        return streamOutgoing.OnNextAsync(txId);
                    }
                    case TransactionType.PRICE_UPDATE:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var grainIDStr = grainID.ToString();
                        // logger.LogInformation("Seller worker {0} will be spawned!", grainIDStr);
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainIDStr);
                        return streamOutgoing.OnNextAsync(txId);
                    }
                    // seller
                    case TransactionType.DELETE_PRODUCT:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].NextValue();
                        var grainIDStr = grainID.ToString();
                        // logger.LogInformation("Seller worker {0} will be spawned!", grainIDStr);
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainIDStr);
                        return streamOutgoing.OnNextAsync(txId);
                    }
                    default:
                    {
                        long threadId = Environment.CurrentManagedThreadId;
                        this.logger.LogError("Thread ID " + threadId + " Unknown transaction type defined!");
                        break;
                    }
                }
            }
            catch (Exception e)
            {
                long threadId = Environment.CurrentManagedThreadId;
                this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
            }
            
            return Task.CompletedTask;
            
        }

    }
}