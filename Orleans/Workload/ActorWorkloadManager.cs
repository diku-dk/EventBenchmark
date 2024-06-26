﻿using System.Diagnostics;
using Common.Services;
using Common.Workload;
using Microsoft.Extensions.Logging;

namespace Orleans.Workload;

public sealed class ActorWorkloadManager : WorkloadManager
{
    private ActorWorkloadManager(
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(sellerService, customerService, deliveryService, transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
    }

    public static new ActorWorkloadManager BuildWorkloadManager(
            ISellerService sellerService,
            ICustomerService customerService,
            IDeliveryService deliveryService,
            IDictionary<TransactionType, int> transactionDistribution,
            Interval customerRange,
            int concurrencyLevel,
            int executionTime,
            int delayBetweenRequests)
    {
        return new ActorWorkloadManager(sellerService, customerService, deliveryService, transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests);
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
        Stopwatch s = new Stopwatch();
        var tasks = new List<Task>(this.concurrencyLevel);
        var execTime = TimeSpan.FromMilliseconds(this.executionTime);
        int currentTid = 1;
        var startTime = DateTime.UtcNow;
        this.logger.LogInformation("Started sending batch of transactions with concurrency level {0} at {0}.", this.concurrencyLevel, startTime);
        s.Start();
        while (currentTid < this.concurrencyLevel)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            //histogram[tx]++;
            var toPass = currentTid;
            tasks.Add( Task.Run(()=> SubmitTransaction(toPass.ToString(), tx)) );
            currentTid++;
        }

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
        }

        var finishTime = DateTime.UtcNow;
        s.Stop();
        this.logger.LogInformation("Run finished at {0}.", finishTime);
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
        return (startTime, finishTime);
    }

    // signal when all threads have started
    private Barrier barrier;
    private CountdownEvent countdown;

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
        
        int currentTid = 0;

        this.barrier.SignalAndWait();

        while(!countdown.IsSet)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            this.SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        }

        Console.WriteLine("Thread {0} finished", threadId);
    }

}