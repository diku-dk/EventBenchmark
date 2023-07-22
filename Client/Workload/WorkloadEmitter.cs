using System.Collections.Concurrent;
using System.Diagnostics;
using Common.Distribution;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Runtime;
using MathNet.Numerics.Distributions;

namespace Client.Workload
{

    public class WorkloadEmitter
	{

        private readonly IClusterClient orleansClient;

        private readonly IDictionary<TransactionType, int> transactionDistribution;
        private readonly Random random;

        private readonly IStreamProvider streamProvider;

        public readonly Dictionary<TransactionType, IDiscreteDistribution> keyGeneratorPerWorkloadType;

        private readonly ConcurrentDictionary<long, CustomerWorkerStatus> customerStatusCache;

        private readonly int executionTime;
        private readonly int concurrencyLevel;

        private readonly int delayBetweenRequests;

        private readonly ILogger logger;

        private readonly IDictionary<TransactionType, int> histogram;

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
            this.histogram = new Dictionary<TransactionType,int>()
            {
                [TransactionType.PRICE_UPDATE] = 0,
                [TransactionType.DELETE_PRODUCT] = 0,
                [TransactionType.DASHBOARD] = 0,
                [TransactionType.CUSTOMER_SESSION] = 0,
                [TransactionType.UPDATE_DELIVERY] = 0
            };
            IDiscreteDistribution sellerIdGenerator = 
                                sellerDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                new Zipf(0.80, sellerRange.max, new Random());

            IDiscreteDistribution customerIdGenerator = 
                                customerDistribution == DistributionType.UNIFORM ?
                                    new DiscreteUniform(customerRange.min, customerRange.max, new Random()) :
                                    new Zipf(0.80, customerRange.max, new Random());

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

        // volatile because for some reason the runtime is sending old TIDs, making it duplicated inside the workers...
        private int currentTid = 1;

        // two classes of transactions:
        // a.eventual complete
        // b. complete on response received
        // for b it is easy, upon completion we know we can submit another transaction
        // for a is tricky, we never know when it completes
        public async Task<(DateTime startTime, DateTime finishTime)> Run()
		{

            //await SetUpCustomerWorkerListener();

            logger.LogInformation("[Workload emitter] Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

            Stopwatch s = new Stopwatch();
            var execTime = TimeSpan.FromMilliseconds(executionTime);

            var startTime = DateTime.UtcNow;
            s.Start();
            while (this.currentTid < concurrencyLevel)
            {
                TransactionType tx = PickTransactionFromDistribution();
                histogram[tx]++;
                var txId = new TransactionInput(this.currentTid, tx);
                SubmitTransaction(txId);
                this.currentTid++;
            }

            logger.LogInformation("[Workload emitter] Started waiting for results to send remaining transactions...");

            while (s.Elapsed < execTime)
            {
                // wait for results
                // logger.LogInformation("[Workload emitter] Retrieving a result...");
                TransactionType tx = PickTransactionFromDistribution();
                histogram[tx]++;
                var txId = new TransactionInput(this.currentTid, tx);
                SubmitTransaction(txId);
                this.currentTid++;

                if (!Shared.ResultQueue.Reader.TryRead(out var _))
                {
                    // either read or wake up when run finishes...
                    await Task.WhenAny(Shared.ResultQueue.Reader.ReadAsync().AsTask(), Task.Delay(execTime - s.Elapsed)).ConfigureAwait(true);
                }

                // throttle
                if (this.delayBetweenRequests > 0)
                {
                    await Task.Delay(this.delayBetweenRequests).ConfigureAwait(true);
                }
                
            }

            var finishTime = DateTime.UtcNow;
            s.Stop();

            logger.LogInformation("[Workload emitter] Finished at {0}. Last TID submitted was {1}", finishTime, currentTid);
            logger.LogInformation("[Workload emitter] Histogram:");
            foreach(var entry in histogram)
            {
                logger.LogInformation("{0}: {1}", entry.Key, entry.Value);
            }

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

        private static readonly byte ITEM = 0;

        private void SubmitTransaction(TransactionInput txId)
        {
            // this.logger.LogInformation("Sending a new {0} transaction with ID {1}", txId.type, txId.tid);
            try
            {
                switch (txId.type)
                {
                    case TransactionType.CUSTOMER_SESSION:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].Sample();
                        // but make sure there is no active session for the customer. if so, pick another customer
                        int count = 1;
                        while ((this.customerStatusCache.ContainsKey(grainID) &&
                                !customerStatusCache.TryUpdate(grainID, CustomerWorkerStatus.BROWSING, CustomerWorkerStatus.IDLE)) ||
                                !customerStatusCache.TryAdd(grainID, CustomerWorkerStatus.BROWSING))
                        {
                            if (count == 10)
                            {
                                logger.LogWarning("[Workload emitter] Could not find an available customer! Perhaps should increase the number of customer next time?");
                                while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
                                return;
                            }
                            grainID = this.keyGeneratorPerWorkloadType[txId.type].Sample();
                            count++;
                        }

                        var streamId = StreamId.Create(StreamingConstants.CustomerWorkerNameSpace, grainID.ToString());
                        var streamOutgoing = this.streamProvider.GetStream<int>(streamId);

                        streamOutgoing.OnNextAsync(txId.tid).ContinueWith(x => {
                            this.customerStatusCache[grainID] = CustomerWorkerStatus.IDLE;
                        });
                        break;
                    }
                    // delivery worker
                    case TransactionType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryWorkerNameSpace, "0");
                        streamOutgoing.OnNextAsync(txId.tid).ContinueWith(x=> Shared.ResultQueue.Writer.WriteAsync(ITEM));
                        break;
                    }
                    // seller worker
                    case TransactionType.DASHBOARD:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].Sample();
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainID.ToString());
                        streamOutgoing.OnNextAsync(txId).ContinueWith(x => Shared.ResultQueue.Writer.WriteAsync(ITEM));
                        break;
                    }
                    case TransactionType.PRICE_UPDATE:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].Sample();
                        var grainIDStr = grainID.ToString();
                        // logger.LogInformation("Seller worker {0} will be spawned!", grainIDStr);
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainIDStr);
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
                    }
                    // seller
                    case TransactionType.DELETE_PRODUCT:
                    {
                        long grainID = this.keyGeneratorPerWorkloadType[txId.type].Sample();
                        var grainIDStr = grainID.ToString();
                        // logger.LogInformation("Seller worker {0} will be spawned!", grainIDStr);
                        var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainIDStr);
                        _ = streamOutgoing.OnNextAsync(txId);
                        break;
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

        }

    }
}