using System;
using System.Collections.Generic;
using System.Threading;
using Common.Services;
using Common.Workload;

namespace Modb;

public sealed class ModbWorkloadManager : WorkloadManager
{
    public static new ModbWorkloadManager BuildWorkloadManager(
                ISellerService sellerService,
                ICustomerService customerService,
                IDeliveryService deliveryService,
                IDictionary<TransactionType, int> transactionDistribution,
                Interval customerRange,
                int concurrencyLevel,
                int executionTime,
                int delayBetweenRequests)
    {
        return new ModbWorkloadManager(sellerService, customerService, deliveryService, transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests);
    }

    // signal when all threads have started
    private Barrier barrier;
    private CancellationTokenSource tokenSource;

    public ModbWorkloadManager(
                ISellerService sellerService,
                ICustomerService customerService,
                IDeliveryService deliveryService,
                IDictionary<TransactionType,int> transactionDistribution,
                Interval customerRange,
                int concurrencyLevel,
                int executionTime,
                int delayBetweenRequests) : base(sellerService, customerService, deliveryService, transactionDistribution, customerRange,
                    concurrencyLevel, executionTime, delayBetweenRequests)
    {

    }

    public (DateTime startTime, DateTime finishTime) RunThreads(CancellationTokenSource cancellationTokenSource)
    {
        int i = 0;
        // pass token source along to align polling task and emitter threads
        this.tokenSource = cancellationTokenSource;
        this.barrier = new Barrier(this.concurrencyLevel+1);

        while(i < this.concurrencyLevel)
        {
            var thread = new Thread(Worker);
            thread.Start();
            i++;
        }

        // for all to start at the same time
        this.barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        Console.WriteLine("Run started at {0}.", startTime);
        Thread.Sleep(this.executionTime);
        cancellationTokenSource.Cancel();
        var finishTime = DateTime.UtcNow;
        this.barrier.Dispose();
        Console.WriteLine("Run finished at {0}.", finishTime);
        return (startTime, finishTime);
    }

    private void Worker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("Thread {0} started", threadId); 
        int currentTid = 0;
        this.barrier.SignalAndWait();
        while(!tokenSource.IsCancellationRequested)
        {
            TransactionType tx = this.PickTransactionFromDistribution();
            this.SubmitTransaction(threadId+"-"+(currentTid++).ToString(), tx);
        }
        Console.WriteLine("Thread {0} finished", threadId);
    }

}


