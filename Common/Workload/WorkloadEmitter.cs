using System.Collections.Concurrent;
using System.Diagnostics;
using Common.Distribution;
using Common.Infra;
using Common.Workload;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using MathNet.Numerics.Distributions;

namespace Common.Workload;


public abstract class WorkloadEmitter
{
    protected static readonly byte ITEM = 0;

    private readonly IDictionary<TransactionType, int> transactionDistribution;
    private readonly Random random;     

    protected readonly Dictionary<TransactionType, IDiscreteDistribution> keyGeneratorPerWorkloadType;

    protected readonly ConcurrentDictionary<int, WorkerStatus> customerStatusCache;
    protected readonly ConcurrentDictionary<int, WorkerStatus> sellerStatusCache;

    private readonly int executionTime;
    private readonly int concurrencyLevel;

    private readonly int delayBetweenRequests;

    protected readonly ILogger logger;

    private readonly IDictionary<TransactionType, int> histogram;

    public WorkloadEmitter(
                            IDictionary<TransactionType,int> transactionDistribution,
                            DistributionType sellerDistribution,
                            Interval sellerRange,
                            DistributionType customerDistribution,
                            Interval customerRange,
                            int concurrencyLevel,
                            int executionTime,
                            int delayBetweenRequests) 
    {
          
        this.transactionDistribution = transactionDistribution;
        this.random = new Random();
            
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

        this.sellerStatusCache = new();
        for (int i = sellerRange.min; i < sellerRange.max; i++)
        {
            this.sellerStatusCache.TryAdd(i, WorkerStatus.IDLE);
        }
        this.customerStatusCache = new();
        for (int i = customerRange.min; i < customerRange.max; i++)
        {
            this.customerStatusCache.TryAdd(i, WorkerStatus.IDLE);
        }

        this.keyGeneratorPerWorkloadType = new()
        {
            [TransactionType.PRICE_UPDATE] = sellerIdGenerator,
            [TransactionType.DELETE_PRODUCT] = sellerIdGenerator,
            [TransactionType.DASHBOARD] = sellerIdGenerator,
            [TransactionType.CUSTOMER_SESSION] = customerIdGenerator
        };

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
        logger.LogInformation("[Workload emitter] Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

        Stopwatch s = new Stopwatch();
        var execTime = TimeSpan.FromMilliseconds(executionTime);

        var startTime = DateTime.UtcNow;
        s.Start();
        while (this.currentTid < concurrencyLevel)
        {
            TransactionType tx = PickTransactionFromDistribution();
            histogram[tx]++;
            SubmitTransaction(this.currentTid, tx);
            this.currentTid++;
        }

        logger.LogInformation("[Workload emitter] Started waiting for results to send remaining transactions...");

        while (s.Elapsed < execTime)
        {
            TransactionType tx = PickTransactionFromDistribution();
            histogram[tx]++;
            
            // spawning in a different thread may lead to duplicate tids in actors
            SubmitTransaction(this.currentTid, tx);
            this.currentTid++;

            while (!Shared.ResultQueue.Reader.TryRead(out var _) && s.Elapsed < execTime) { }

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

    protected abstract void SubmitTransaction(int tid, TransactionType type);

}
