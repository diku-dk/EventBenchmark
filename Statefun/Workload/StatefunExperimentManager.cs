using Common.Experiment;
using Common.Workload;
using Common.Infra;
using Common.Entities;
using Common.Services;
using Common.Workers.Seller;
using Common.Workers.Customer;
using DuckDB.NET.Data;
using Statefun.Workers;
using Common.Metric;

namespace Statefun.Workload;

public sealed class StatefunExperimentManager : AbstractExperimentManager
{        
    private string receiptUrl = "http://statefunhost:8091/receipts";

    private CancellationTokenSource cancellationTokenSource;
    
    private readonly IHttpClientFactory httpClientFactory;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    private readonly Dictionary<int, AbstractCustomerWorker> customerThreads;
    private readonly StatefunDeliveryThread deliveryThread;

    private int numSellers;

    private readonly WorkloadManager workloadManager;
    private readonly MetricManager metricManager;

    // private readonly StatefunReceiptPullingThread receiptPullingThread;
    // define a pulling thread list which contains 3 pulling threads
    private readonly List<StatefunReceiptPullingThread> receiptPullingThreads;

    private static readonly int numPullingThreads = 3;

    public StatefunExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection = null) : base(config, connection)
    {
        this.httpClientFactory = httpClientFactory;

        this.deliveryThread = StatefunDeliveryThread.BuildDeliveryThread(config.deliveryWorkerConfig);
        this.deliveryService = new DeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, AbstractCustomerWorker>();
        this.customerService = new CustomerService(this.customerThreads);

        this.numSellers = 0;

        this.workloadManager = new WorkloadManager(
            sellerService, customerService, deliveryService,
            config.transactionDistribution,
            // set in the base class
            this.customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);

        this.metricManager = new MetricManager(sellerService, customerService, deliveryService);

        this.receiptPullingThreads = new List<StatefunReceiptPullingThread>();
        for (int i = 0; i < numPullingThreads; i++) {
            this.receiptPullingThreads.Add(new StatefunReceiptPullingThread(receiptUrl, customerService, sellerService, deliveryService));
        }

        this.cancellationTokenSource = new CancellationTokenSource();
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        this.metricManager.SetUp(numSellers, config.numCustomers);
        this.metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}_{6}", ts, runIdx, config.numCustomers, config.concurrencyLevel,
                    config.runs[runIdx].numProducts, config.runs[runIdx].sellerDistribution, config.runs[runIdx].keyDistribution));
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
        this.numSellers = (int)DuckDbUtils.Count(this.connection, "sellers");

        for (int i = 1; i <= numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(this.connection, "products", "seller_id = " + i);
            if (!this.sellerThreads.ContainsKey(i))
            {
                this.sellerThreads[i] = StatefunSellerThread.BuildSellerThread(i, httpClientFactory, config.sellerWorkerConfig);
                this.sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
            else
            {
                this.sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
        }

        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = customerRange.min; i <= customerRange.max; i++)
        {
            this.customerThreads[i].SetUp(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }

        // start pulling thread to collect receipts
        foreach (var thread in this.receiptPullingThreads) {
            Task.Factory.StartNew(() => thread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }      
        Console.WriteLine("=== Starting receipt pulling thread ===");
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        this.workloadManager.SetUp(config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return this.workloadManager;
    }

}

