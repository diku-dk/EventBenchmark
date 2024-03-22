using System.Diagnostics;
using Common.Services;
using Common.Workload;
using Microsoft.Extensions.Logging;

namespace Dapr.Workload;

public sealed class ActorWorkloadManager : WorkloadManager
{
    public ActorWorkloadManager(
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(sellerService, customerService, deliveryService, transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
    }

    public (DateTime startTime, DateTime finishTime) RunTasks()
    {
        int numCpus = this.concurrencyLevel;
        int i = 0;
        var tasks = new List<Task>();

        this.countdown = new CountdownEvent(1);
        this.barrier = new Barrier(numCpus+1);
        while(i < numCpus)
        {
            //Console.WriteLine("Init thread {0}", i);
            tasks.Add(Task.Run(TaskWorker));
            i++;
        }

        this.barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        this.logger.LogInformation("Run started at {0}.", startTime);
        Thread.Sleep(this.executionTime);
        this.countdown.Signal();
        var finishTime = DateTime.UtcNow;
        this.barrier.Dispose();
        this.logger.LogInformation("Run finished at {0}.", finishTime);

        return (startTime, finishTime);
    }

    public async Task<(DateTime startTime, DateTime finishTime)> RunTaskPerTx()
	{
        // logger.LogInformation("Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

        Stopwatch s = new Stopwatch();
        var execTime = TimeSpan.FromMilliseconds(this.executionTime);
        int currentTid = 0;
        var startTime = DateTime.UtcNow;
        this.logger.LogInformation("Run started at {0}.", startTime);
        s.Start();

        var tasks = new List<Task>(this.concurrencyLevel);

        while (currentTid < this.concurrencyLevel)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            tasks.Add( Task.Run(()=> SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
        }

        // logger.LogInformation("{0} transactions emitted. Waiting for results to send remaining transactions.", this.concurrencyLevel);

        while (s.Elapsed < execTime)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
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
        this.logger.LogInformation("Run finished at {0}.", finishTime);

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

        this.countdown = new CountdownEvent(1);
        this.barrier = new Barrier(numCpus+1);

        while(i < numCpus)
        {
            var thread = new Thread(ThreadWorker);
            thread.Start();
            i++;
        }

        this.barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        this.logger.LogInformation("Run started at {0}.", startTime);
        Thread.Sleep(this.executionTime);
        this.countdown.Signal();
        var finishTime = DateTime.UtcNow;
        this.barrier.Dispose();
        this.logger.LogInformation("Run finished at {0}.", finishTime);

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

        this.barrier.SignalAndWait();

        while (!countdown.IsSet)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            this.SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
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

        this.barrier.SignalAndWait();

        while(!countdown.IsSet)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            this.SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        }

        //histograms.Add(histogram);
        Console.WriteLine("Thread {0} finished", threadId);
    }

}