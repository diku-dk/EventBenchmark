using Common.Experiment;
using Common.Workload;
using Common.Infra;
using Common.Entities;
using Common.Services;
using Common.Workers.Seller;
using Common.Workers.Customer;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Statefun.Workers;
using Common.DataGeneration;
using Statefun.Infra;
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
    private readonly Dictionary<int, AbstractCustomerThread> customerThreads;
    private readonly StatefunDeliveryThread deliveryThread;

    private int numSellers;

    private readonly StatefunWorkloadManager workloadManager;
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

        this.metricManager = new MetricManager(sellerService, customerService, deliveryService);

        this.receiptPullingThreads = new List<StatefunReceiptPullingThread>();
        for (int i = 0; i < numPullingThreads; i++) {
            this.receiptPullingThreads.Add(new StatefunReceiptPullingThread(receiptUrl, customerService, sellerService, deliveryService));
        }

        this.cancellationTokenSource = new CancellationTokenSource();
    }

    public override async Task Run()
    {
        this.connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = config.numCustomers,
            numProducts = 0 // to force product generation and ingestion in the upcoming loop
        };

        int runIdx = 0;
        int lastRunIdx = config.runs.Count() - 1;

        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(connection);
        // dont need to generate customers on every run. only once
        dataGen.GenerateCustomers(connection);
        // customers are fixed accross runs
        this.customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");

        PreExperiment();

        foreach (var run in config.runs)
        {
            logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.UtcNow);

            if (run.numProducts != previousData.numProducts)
            {
                logger.LogInformation("Run #{0} number of products changed from last run {0}", runIdx, runIdx - 1);

                // update previous
                previousData = new SyntheticDataSourceConfig()
                {
                    numProdPerSeller = config.numProdPerSeller,
                    numCustomers = config.numCustomers,
                    numProducts = run.numProducts,
                    qtyPerProduct = config.qtyPerProduct
                };
                var syntheticDataGenerator = new SyntheticDataGenerator(previousData);

                // must truncate if not first run
                if (runIdx > 0)
                    syntheticDataGenerator.TruncateTables(connection);

                syntheticDataGenerator.Generate(connection);

                await CustomIngestionOrchestrator.Run(connection, config.ingestionConfig);
    
                if (runIdx == 0)
                {
                    // remove customers from ingestion config from now on
                    config.ingestionConfig.mapTableToUrl.Remove("customers");
                }

            }

            PreWorkload(runIdx);

            WorkloadManager workloadManager = SetUpManager(runIdx);

            logger.LogInformation("Run #{0} started at {1}", runIdx, DateTime.UtcNow);

            var workloadTask = await workloadManager.Run();

            DateTime startTime = workloadTask.startTime;
            DateTime finishTime = workloadTask.finishTime;

            logger.LogInformation("Wait for microservices to converge (i.e., finish receiving events) for {0} seconds...", config.delayBetweenRuns / 1000);
            await Task.Delay(config.delayBetweenRuns);

            cancellationTokenSource.Cancel();

            // set up data collection for metrics
            Collect(runIdx, startTime, finishTime);            

            CollectGarbage();

            logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

            // increment run index
            runIdx++;

            if(runIdx < (config.runs.Count - 1))
                PostRunTasks(runIdx, lastRunIdx);
        }

        logger.LogInformation("Post experiment cleanup tasks started.");

        PostExperiment();

        logger.LogInformation("Experiment finished");
    }

    public override async Task RunSimpleExperiment(int type)
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.SetUpManager(0);
        (DateTime startTime, DateTime finishTime) res;
        if(type == 0){
            Console.WriteLine("Thread mode selected.");
            res = this.workloadManager.RunThreads();
        }
        else if(type == 1) {
            Console.WriteLine("Task mode selected.");
            res = this.workloadManager.RunTasks();
        }
        else {
            Console.WriteLine("Task per Tx mode selected.");
            res = await this.workloadManager.RunTaskPerTx();
        }
        DateTime startTime = res.startTime;
        DateTime finishTime = res.finishTime;
        this.Collect(0, startTime, finishTime);
        this.PostExperiment();
        this.CollectGarbage();
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
        // cancellationTokenSource.Cancel();
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
        // Task.Factory.StartNew(() => receiptPullingThread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        foreach (var thread in receiptPullingThreads) {
            Task.Factory.StartNew(() => thread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }
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


