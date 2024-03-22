using Common.Workload;
using Common.Services;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Statefun.Workload;

public sealed class StatefunWorkloadManager : WorkloadManager
{
    private static int totalTransactionsSubmitted = 0;
 
    // signal when all threads have started
    private Barrier barrier;
    private CountdownEvent countdown;

    public StatefunWorkloadManager(
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
        totalTransactionsSubmitted = 0;

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

    public (DateTime startTime, DateTime finishTime) RunThreads()
    {
        int numCpus = this.concurrencyLevel;
        int i = 0;
        totalTransactionsSubmitted = 0;

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

    public async Task<(DateTime startTime, DateTime finishTime)> RunTaskPerTx()
	{
        var startTime = DateTime.UtcNow;
        this.logger.LogInformation("Started sending batch of transactions with concurrency level {0}  at {0}.", this.concurrencyLevel, startTime);

        Stopwatch s = new Stopwatch();
        var execTime = TimeSpan.FromMilliseconds(executionTime);
        int currentTid = 0;
        var tasks = new List<Task>(this.concurrencyLevel);

        s.Start();
        while (currentTid < concurrencyLevel)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            tasks.Add( Task.Run(()=> this.SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
        }

        // logger.LogInformation("{0} transactions emitted. Waiting for results to send remaining transactions.", this.concurrencyLevel);

        while (s.Elapsed < execTime)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            // spawning in a different thread may lead to duplicate tids in actors
            tasks.Add( Task.Run(()=> this.SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
            try
            {
                // FIXME perhaps no need to wait for sending task completion?
                // the reception of a transaction result already means a sending task has completed
                var t = await Task.WhenAny(tasks).WaitAsync(execTime - s.Elapsed);
                tasks.Remove(t);
            }
            catch (TimeoutException) { }

            while (!Shared.ResultQueue.Reader.TryRead(out _) && s.Elapsed < execTime) { }

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

    private void TaskWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("Thread {0} started", threadId);        

        barrier.SignalAndWait();

        int currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);
        while (currentTid < concurrencyLevel)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            this.SubmitTransaction(toPass.ToString(), tx);     
            currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);       
        }

        while(!countdown.IsSet)
        {        
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            this.SubmitTransaction(currentTid.ToString(), tx);
            while (!Shared.ResultQueue.Reader.TryRead(out _) && !countdown.IsSet) { }
            currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);
        }
        Console.WriteLine("Thread {0} finished", threadId);
    }

    private void ThreadWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("Thread {0} started", threadId);
        
        barrier.SignalAndWait();

        int currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);
        while (currentTid < concurrencyLevel)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            this.SubmitTransaction(toPass.ToString(), tx);     
            currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);       
        }

        while(!countdown.IsSet)
        {        
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            this.SubmitTransaction(currentTid.ToString(), tx);
            while (!Shared.ResultQueue.Reader.TryRead(out _) && !countdown.IsSet) { }
            currentTid = Interlocked.Increment(ref totalTransactionsSubmitted);
        }

        //histograms.Add(histogram);
        Console.WriteLine("Thread {0} finished", threadId);
    }

}
 