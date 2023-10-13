using System.Diagnostics;
using Common.Services;
using Common.Workload;
using Microsoft.Extensions.Logging;

namespace Dapr.Workload;

public class ActorWorkloadManager : WorkloadManager
{
    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;

    public ActorWorkloadManager(
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
    }

    public (DateTime startTime, DateTime finishTime) RunTasks()
    {
        int numCpus = this.concurrencyLevel;
        int i = 0;
        var tasks = new List<Task>();

        countdown = new CountdownEvent(1);
        barrier = new Barrier(numCpus+1);
        while(i < numCpus)
        {
            //Console.WriteLine("Init thread {0}", i);
            tasks.Add(Task.Run(TaskWorker));
            i++;
        }

        barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        logger.LogInformation("Run started at {0}.", startTime);
        Thread.Sleep(this.executionTime);
        countdown.Signal();
        var finishTime = DateTime.UtcNow;
        barrier.Dispose();
        logger.LogInformation("Run finished at {0}.", finishTime);

        return (startTime, finishTime);
    }

    public async Task<(DateTime startTime, DateTime finishTime)> RunTaskPerTx()
	{
        // logger.LogInformation("Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

        Stopwatch s = new Stopwatch();
        var execTime = TimeSpan.FromMilliseconds(executionTime);
        int currentTid = 0;
        var startTime = DateTime.UtcNow;
        logger.LogInformation("Run started at {0}.", startTime);
        s.Start();

        var tasks = new List<Task>(concurrencyLevel);

        while (currentTid < concurrencyLevel)
        {
            TransactionType tx = PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            tasks.Add( Task.Run(()=> SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
        }

        // logger.LogInformation("{0} transactions emitted. Waiting for results to send remaining transactions.", this.concurrencyLevel);

        while (s.Elapsed < execTime)
        {
            TransactionType tx = PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            // spawning in a different thread may lead to duplicate tids in actors
            tasks.Add( Task.Run(()=> SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
            try
            {
                var t = await Task.WhenAny(tasks).WaitAsync(execTime - s.Elapsed);
                tasks.Remove(t);
            }
            catch (TimeoutException) { }

            //var t = await Task.WhenAny( tasks );
            //tasks.Remove(t);

        }

        var finishTime = DateTime.UtcNow;
        s.Stop();
        logger.LogInformation("Run finished at {0}.", finishTime);

        //logger.LogInformation("[Workload emitter] Finished at {0}. Last TID submitted was {1}", finishTime, currentTid);
        //logger.LogInformation("[Workload emitter] Histogram:");
        //foreach(var entry in histogram)
        //{
        //    logger.LogInformation("{0}: {1}", entry.Key, entry.Value);
        //}

        return (startTime, finishTime);
    }

    public (DateTime startTime, DateTime finishTime) RunThreads()
    {
        int numCpus = this.concurrencyLevel;
        int i = 0;

        countdown = new CountdownEvent(1);
        barrier = new Barrier(numCpus+1);

        while(i < numCpus)
        {
            var thread = new Thread(ThreadWorker);
            thread.Start();
            i++;
        }

        barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        logger.LogInformation("Run started at {0}.", startTime);
        Thread.Sleep(this.executionTime);
        countdown.Signal();
        var finishTime = DateTime.UtcNow;
        barrier.Dispose();
        logger.LogInformation("Run finished at {0}.", finishTime);

        //Thread.Sleep(2000);

        //logger.LogInformation("Histogram (#{0} entries):", histograms.Count);
        //while(histograms.TryTake(out var threadEntry))
        //{
        //    foreach(var entry in threadEntry)
        //    {
        //        histogram[entry.Key] =+ entry.Value;
        //    }
        //}
        //foreach(var entry in histogram)
        //{
        //    logger.LogInformation("{0}: {1}", entry.Key, entry.Value);
        //}

        return (startTime, finishTime);
    }

    // signal when all threads have started
    private Barrier barrier;
    private CountdownEvent countdown;

    //private readonly BlockingCollection<Dictionary<TransactionType, int>> histograms = new BlockingCollection<Dictionary<TransactionType, int>>();

    private void TaskWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("Thread {0} started", threadId);
        int currentTid = 0;

        barrier.SignalAndWait();

        while (!countdown.IsSet)
        {
            TransactionType tx = PickTransactionFromDistribution();
            SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        }
        Console.WriteLine("Thread {0} finished", threadId);
    }

    private void ThreadWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("Thread {0} started", threadId);
        //var histogram = new Dictionary<TransactionType, int>();
        //foreach (TransactionType tx in Enum.GetValues(typeof(TransactionType)))
        //{
        //    histogram.Add(tx, 0);
        //}
        
        int currentTid = 0;

        barrier.SignalAndWait();

        while(!countdown.IsSet)
        {
            TransactionType tx = PickTransactionFromDistribution();
            //histogram[tx]++;
            SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        }

        //histograms.Add(histogram);
        Console.WriteLine("Thread {0} finished", threadId);
    }

    protected override void SubmitTransaction(string tid, TransactionType type)
    {
        try
        {
            switch (type)
            {
                case TransactionType.CUSTOMER_SESSION:
                {
                    int customerId = this.customerIdleQueue.Take();
                    this.customerService.Run(customerId, tid);
                    this.customerIdleQueue.Add(customerId);
                    break;
                }
                // delivery worker
                case TransactionType.UPDATE_DELIVERY:
                {
                    this.deliveryService.Run(tid);
                    break;
                }
                // seller worker
                case TransactionType.PRICE_UPDATE:
                case TransactionType.UPDATE_PRODUCT:
                case TransactionType.QUERY_DASHBOARD:
                {
                    int sellerId = this.sellerIdGenerator.Sample();
                    this.sellerService.Run(sellerId, tid, type);
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