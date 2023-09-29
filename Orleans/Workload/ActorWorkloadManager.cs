using System.Collections.Concurrent;
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

    public override async Task<(DateTime startTime, DateTime finishTime)> Run()
    {
        int numCpus = this.concurrencyLevel;
        int i = 0;
        var tasks = new List<Task>();

        

        countdown = new CountdownEvent(1);
        barrier = new Barrier(numCpus+1);
        while(i < numCpus)
        {
            //Console.WriteLine("Init thread {0}", i);
            var thread = new Thread(WorkloadWorker);
            thread.Start();
            i++;
        }
        barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        logger.LogInformation("Run started at {0}.", startTime);
        // await Task.Delay(this.executionTime);
        //Console.WriteLine("Sleeping Run()");
        // Thread.Sleep(this.executionTime);
        await Task.Delay(this.executionTime);
        countdown.Signal();
        var finishTime = DateTime.UtcNow;
        barrier.Dispose();
        // Console.WriteLine("Finished Run()");

        logger.LogInformation("Run finished at {0}.", finishTime);
        logger.LogInformation("Histogram (#{0} entries):", histograms.Count);
        while(histograms.TryTake(out var threadEntry))
        {
            foreach(var entry in threadEntry)
            {
                histogram[entry.Key] =+ entry.Value;
            }
        }
        foreach(var entry in histogram)
        {
            logger.LogInformation("{0}: {1}", entry.Key, entry.Value);
        }

        return (startTime, finishTime);
    }

    // signal when all threads have started
    private Barrier barrier;
    private CountdownEvent countdown;

    private readonly BlockingCollection<Dictionary<TransactionType, int>> histograms = new BlockingCollection<Dictionary<TransactionType, int>>();

    private void WorkloadWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("I am thread {0}", threadId);
        var histogram = new Dictionary<TransactionType, int>();
        foreach (TransactionType tx in Enum.GetValues(typeof(TransactionType)))
        {
            histogram.Add(tx, 0);
        }
        
        int currentTid = 0;

        barrier.SignalAndWait();

        while(!countdown.IsSet)
        {
            TransactionType tx = PickTransactionFromDistribution();
            histogram[tx]++;
            SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        }

        histograms.Add(histogram);
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