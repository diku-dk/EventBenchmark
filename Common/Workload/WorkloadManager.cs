using System.Collections.Concurrent;
using System.Diagnostics;
using Common.Distribution;
using Common.Infra;
using Microsoft.Extensions.Logging;
using MathNet.Numerics.Distributions;

namespace Common.Workload;

public abstract class WorkloadManager
{
    public static readonly byte ITEM = 0;

    private readonly IDictionary<TransactionType, int> transactionDistribution;
    private readonly Random random;

    //protected readonly ConcurrentDictionary<int, WorkerStatus> customerStatusCache;
    protected readonly BlockingCollection<int> customerIdleQueue;

    private readonly int executionTime;
    private readonly int concurrencyLevel;

    private readonly int delayBetweenRequests;

    protected readonly ILogger logger;

    private readonly IDictionary<TransactionType, int> histogram;

    protected IDiscreteDistribution sellerIdGenerator;

    private readonly Interval customerRange;

    public WorkloadManager(IDictionary<TransactionType,int> transactionDistribution,
                            Interval customerRange,
                            int concurrencyLevel,
                            int executionTime,
                            int delayBetweenRequests) 
    {
        this.transactionDistribution = transactionDistribution;
        this.customerRange = customerRange;
        this.random = new Random();
        this.concurrencyLevel = concurrencyLevel;
        this.executionTime = executionTime;
        this.delayBetweenRequests = delayBetweenRequests;
        this.histogram = new Dictionary<TransactionType, int>();
        this.customerIdleQueue = new BlockingCollection<int>(new ConcurrentQueue<int>());
        this.logger = LoggerProxy.GetInstance("WorkloadEmitter");
    }

    // can differ across runs
    public void SetUp(DistributionType sellerDistribution, Interval sellerRange)
    {
        this.sellerIdGenerator =
                    sellerDistribution == DistributionType.UNIFORM ?
                    new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                    new Zipf(0.80, sellerRange.max, new Random());

        foreach (TransactionType tx in Enum.GetValues(typeof(TransactionType)))
        {
            this.histogram[tx] = 0;
        }

        while(this.customerIdleQueue.TryTake(out _)){ }
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerIdleQueue.Add(i);
        }

    }

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
            var toPass = this.currentTid;
            SubmitTransaction(toPass, tx);
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

            while (!Shared.ResultQueue.Reader.TryRead(out _) && s.Elapsed < execTime) { }

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
