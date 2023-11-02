using Common.Experiment;
using Common.Workload;
using Common.Infra;
using Common.Entities;
using Common.Services;
using Common.Workers;
using Common.Workers.Seller;
using Common.Workers.Customer;
using Statefun.Metric; 
using DuckDB.NET.Data;
using Statefun.Workers;

namespace Statefun.Workload;

public class StatefunExperimentManager : ExperimentManager
{        
    private string receiptUrl = "http://localhost:8091/receipts";

    private CancellationTokenSource cancellationTokenSource;
    
    private readonly IHttpClientFactory httpClientFactory;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    private readonly Dictionary<int, AbstractCustomerThread> customerThreads;
    private readonly StatefunDeliveryThread deliveryThread;

    private int numSellers;

    private readonly StatefunWorkloadManager workloadManager;
    private readonly StatefunMetricManager metricManager;

    private readonly StatefunReceiptPullingThread receiptPullingThread;

    public StatefunExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection = null) : base(config, connection)
    {
        this.httpClientFactory = httpClientFactory;

        // this.deliveryThread = new StatefunDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        this.deliveryThread = StatefunDeliveryThread.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        // this.deliveryThread = DeliveryThread.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        this.deliveryService = new DeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, AbstractCustomerThread>();
        this.customerService = new CustomerService(this.customerThreads);

        this.numSellers = 0;

        this.workloadManager = new StatefunWorkloadManager(
            sellerService, customerService, deliveryService,
            config.transactionDistribution,
            // set in the base class
            this.customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);

        this.metricManager = new StatefunMetricManager(sellerService, customerService, deliveryService);

        this.receiptPullingThread = new StatefunReceiptPullingThread(receiptUrl, customerService, sellerService, deliveryService);
        this.cancellationTokenSource = new CancellationTokenSource();
    }

    public async Task RunSimpleExperiment(int type)
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
        PreExperiment();
        PreWorkload(0);
        SetUpManager(0);
        (DateTime startTime, DateTime finishTime) res;
        if(type == 0){
            Console.WriteLine("Thread mode selected.");
            res = workloadManager.RunThreads();
        }
        else if(type == 1) {
            Console.WriteLine("Task mode selected.");
            res = workloadManager.RunTasks();
        }
        else {
            Console.WriteLine("Task per Tx mode selected.");
            res = await workloadManager.RunTaskPerTx();
        }
        DateTime startTime = res.startTime;
        DateTime finishTime = res.finishTime;
        Collect(0, startTime, finishTime);
        PostExperiment();
        CollectGarbage();
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        this.metricManager.SetUp(numSellers, config.numCustomers);
        this.metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}_{6}", ts, runIdx, config.numCustomers, config.concurrencyLevel,
                    config.runs[runIdx].numProducts, config.runs[runIdx].sellerDistribution, config.runs[runIdx].keyDistribution));
    }

    protected override void PostExperiment()
    {
        cancellationTokenSource.Cancel();
    }

    protected override void PostRunTasks(int runIdx, int lastRunIdx)
    {
        throw new NotImplementedException();
    }

    protected override void PreExperiment()
    {
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, StatefunCustomerThread.BuildCustomerThread(httpClientFactory, sellerService, config.numProdPerSeller, config.customerWorkerConfig, this.customers[i-1]));
        }
    }

    protected override void PreWorkload(int runIdx)
    {
        this.numSellers = (int)DuckDbUtils.Count(connection, "sellers");

        for (int i = 1; i <= numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!sellerThreads.ContainsKey(i))
            {
                sellerThreads[i] = StatefunSellerThread.BuildSellerThread(i, httpClientFactory, config.sellerWorkerConfig);
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
            else
            {
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
        }

        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = customerRange.min; i <= customerRange.max; i++)
        {
            this.customerThreads[i].SetUp(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }

        // start pulling thread to collect receipts
        Task.Factory.StartNew(() => receiptPullingThread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        // Thread thread = new Thread(() => receiptPullingThread.Run(cancellationTokenSource.Token));        
        Console.WriteLine("=== Starting receipt pulling thread ===");
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        this.workloadManager.SetUp(config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return workloadManager;
    }

    protected override void TrimStreams()
    {
        throw new NotImplementedException();
    }
}


