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

    int numCpus = 8;

    public override async Task<(DateTime startTime, DateTime finishTime)> Run()
    {
        int i = 0;
        var tasks = new List<Task>();
        countdown = new CountdownEvent(1);
        barrier = new Barrier(numCpus+1);
        while(i < numCpus)
        {
            Console.WriteLine("Init thread {0}", i);
            var thread = new Thread(WorkloadWorker);
            thread.Start();
            i++;
        }
        barrier.SignalAndWait();
        var startTime = DateTime.UtcNow;
        // await Task.Delay(this.executionTime);
        Console.WriteLine("Sleeping Run()");
        // Thread.Sleep(this.executionTime);
        await Task.Delay(this.executionTime);
        countdown.Signal();
        var finishTime = DateTime.UtcNow;
        barrier.Dispose();
        Console.WriteLine("Finished Run()");
        return (startTime, finishTime);
    }

    // signal when all threads have started
    private Barrier barrier;
    private CountdownEvent countdown;

    private Dictionary<TransactionType,int> histograms = new Dictionary<TransactionType, int>();

    private void WorkloadWorker()
    {
        long threadId = Environment.CurrentManagedThreadId;
        Console.WriteLine("I am thread {0} execution time should be {1}", threadId, this.executionTime);
        int currentTid = 0;
        // Stopwatch s = new Stopwatch();
        // var execTime = TimeSpan.FromMilliseconds(this.executionTime);

        var tasks = new List<Task>();

        barrier.SignalAndWait();

        //var startTime = DateTime.UtcNow;
        //s.Start();
        //int i = 0;
        //while (i < concurrencyLevel) // && s.Elapsed < execTime) ideally, but usually not the case
        //{
            //TransactionType tx = PickTransactionFromDistribution();
            // histogram[tx]++;
            // FIXME best is to each keep its own histogram and merge it in Run() afterwards
            //histogram.AddOrUpdate(tx, 1, (tx, count) => count + 1);
            //var toPass = currentTid++.ToString();
            //tasks.Add(Task.Run(()=>SubmitTransaction(toPass, tx)));
        //    SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
        //    i++;
        //}

        // while (s.Elapsed < execTime)
        while(!countdown.IsSet)
        {
            TransactionType tx = PickTransactionFromDistribution();
            // histogram[tx]++;
            //histogram.AddOrUpdate(tx, 1, (tx, count) => count + 1);
            // spawning in a different thread may lead to duplicate tids in actors
            // var toPass = currentTid++.ToString();
            //tasks.Add(Task.Run(()=>SubmitTransaction(toPass, tx)));
       
            SubmitTransaction(threadId+"-"+currentTid++.ToString(), tx);
            //Task finished = await Task.WhenAny(tasks);
            //tasks.Remove(finished);
        }

        //var finishTime = DateTime.UtcNow;
        //s.Stop();
    }

    //private int currentTid = 1;
    //public async Task<(DateTime startTime, DateTime finishTime)> RunOld()
    //{
    //    logger.LogInformation("[Workload emitter] Started sending batch of transactions with concurrency level {0}", this.concurrencyLevel);

    //    Stopwatch s = new Stopwatch();
    //    var execTime = TimeSpan.FromMilliseconds(this.executionTime);

    //    var tasks = new List<Task>();

    //    var startTime = DateTime.UtcNow;
    //    s.Start();
    //    while (this.currentTid < concurrencyLevel) // && s.Elapsed < execTime) ideally, but usually not the case
    //    {
    //        TransactionType tx = PickTransactionFromDistribution();
    //        histogram[tx]++;
    //        var toPass = this.currentTid;
    //        tasks.Add(Task.Run(()=>SubmitTransaction(toPass, tx)));
    //        this.currentTid++;
    //    }

    //    logger.LogInformation("[Workload emitter] {0} transactions emitted. Waiting for results to send remaining transactions.", this.concurrencyLevel);

    //    while (s.Elapsed < execTime)
    //    {
    //        TransactionType tx = PickTransactionFromDistribution();
    //        histogram[tx]++;
            
    //        // spawning in a different thread may lead to duplicate tids in actors
    //        tasks.Add(Task.Run(()=>SubmitTransaction(this.currentTid, tx)));
    //        this.currentTid++;

    //        // while (!Shared.ResultQueue.Reader.TryRead(out _) && s.Elapsed < execTime) { }
    //        Task finished = await Task.WhenAny(tasks);
    //        tasks.Remove(finished);
    //    }

    //    var finishTime = DateTime.UtcNow;
    //    s.Stop();

    //    logger.LogInformation("[Workload emitter] Finished at {0}. Last TID submitted was {1}", finishTime, currentTid);
    //    logger.LogInformation("[Workload emitter] Histogram:");
    //    foreach(var entry in histogram)
    //    {
    //        logger.LogInformation("{0}: {1}", entry.Key, entry.Value);
    //    }

    //    return (startTime, finishTime);

    //}

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
                    //Task.Run(() => customerService.Run(customerId, tid)).ContinueWith(async x => {
                    //        var t = Shared.ResultQueue.Writer.WriteAsync(ITEM);
                    //        this.customerIdleQueue.Add(customerId);
                    //        await t;
                    //    }); //.ConfigureAwait(true);
                    break;
                }
                // delivery worker
                case TransactionType.UPDATE_DELIVERY:
                {
                    // Task.Run(() => this.deliveryService.Run(tid)).ContinueWith(async x => await Shared.ResultQueue.Writer.WriteAsync(ITEM));
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
                    //Task.Run(() => this.sellerService.Run(sellerId, tid, type)).ContinueWith(async x => await Shared.ResultQueue.Writer.WriteAsync(ITEM));
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