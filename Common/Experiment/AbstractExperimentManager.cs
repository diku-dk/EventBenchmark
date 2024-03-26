using Common.DataGeneration;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Ingestion;
using Common.Metric;
using Common.Services;
using Common.Workers.Customer;
using Common.Workers.Seller;
using Common.Workload;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;
using static Common.Workload.WorkloadManager;

namespace Common.Experiment;

public abstract class AbstractExperimentManager
{

    protected readonly IHttpClientFactory httpClientFactory;

    protected readonly ExperimentConfig config;

    protected readonly DuckDBConnection connection;

    protected static readonly byte ITEM = 0;

    protected static readonly ILogger logger = LoggerProxy.GetInstance("ExperimentManager");

    // workers config
    protected readonly DeliveryService deliveryService;

    protected List<Customer> customers;
    protected readonly Interval customerRange;
    private readonly Dictionary<int, AbstractCustomerWorker> customerThreads;
    protected readonly CustomerService customerService;

    protected readonly SellerService sellerService;
    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    protected int numSellers;

    protected readonly WorkloadManager workloadManager;

    public AbstractExperimentManager(IHttpClientFactory httpClientFactory, BuildWorkloadManagerDelegate workloadManagerDelegate, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        this.httpClientFactory = httpClientFactory;
        this.config = config;
        this.connection = duckDBConnection;

        this.deliveryService = new DeliveryService(deliveryWorkerDelegate(httpClientFactory, config.deliveryWorkerConfig));

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads, sellerWorkerDelegate);
        this.numSellers = 0;

        this.customerThreads = new Dictionary<int, AbstractCustomerWorker>();
        this.customerService = new CustomerService(this.customerThreads, customerWorkerDelegate);
        this.customerRange = new Interval(1, config.numCustomers);

        this.workloadManager = workloadManagerDelegate(
                                this.sellerService, this.customerService, this.deliveryService,
                                config.transactionDistribution,
                                this.customerRange,
                                config.concurrencyLevel,
                                config.executionTime,
                                config.delayBetweenRequests);
    }

    protected virtual void PreExperiment()
    {
        Console.WriteLine("Initializing customer workers...");
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, this.customerService.BuildCustomerWorker(this.httpClientFactory, this.sellerService, this.config.numProdPerSeller, this.config.customerWorkerConfig, this.customers[i - 1]));
        }
    }

    protected virtual void PreWorkload(int runIdx)
    {
        Console.WriteLine("Initializing seller workers...");
        this.numSellers = (int)DuckDbUtils.Count(this.connection, "sellers");
        for (int i = 1; i <= this.numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!this.sellerThreads.ContainsKey(i))
            {
                this.sellerThreads[i] = this.sellerService.BuildSellerWorker(i, this.httpClientFactory, this.config.sellerWorkerConfig); 
            }
            this.sellerThreads[i].SetUp(products, this.config.runs[runIdx].keyDistribution);
        }

        Console.WriteLine("Setting up seller workload info in customer workers...");
        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads[i].SetUp(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }
    }

    protected virtual async void PostRunTasks(int runIdx)
    {
        // reset microservice states
        var resps_ = new List<Task<HttpResponseMessage>>();
        foreach (var task in this.config.postRunTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Post run task to URL {0}", task.url);
            resps_.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps_);
    }

    private async Task TriggerPostExperimentTasks()
    {
        // cleanup microservice states
        var resps_ = new List<Task<HttpResponseMessage>>();
        foreach (var task in this.config.postExperimentTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Post experiment task to URL {0}", task.url);
            resps_.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps_);
    }

    protected virtual async void PostExperiment()
    {
        await this.TriggerPostExperimentTasks();
    }

    protected abstract MetricManager SetUpMetricManager(int runIdx);

    protected virtual void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        var metricManager = this.SetUpMetricManager(runIdx);
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}_{6}", ts, runIdx, this.config.numCustomers, this.config.concurrencyLevel, this.config.runs[runIdx].numProducts, this.config.runs[runIdx].sellerDistribution, this.config.runs[runIdx].keyDistribution));
    }

    public virtual async Task Run()
    {
        this.connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = this.config.numCustomers,
            numProducts = 0 // to force product generation and ingestion in the upcoming loop
        };

        int runIdx = 0;

        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(this.connection);
        // dont need to generate customers on every run. only once
        dataGen.GenerateCustomers(this.connection);
        // customers are fixed accross runs
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");

        this.PreExperiment();

        foreach (var run in this.config.runs)
        {
            logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.UtcNow);

            if (run.numProducts != previousData.numProducts)
            {
                logger.LogInformation("Run #{0} number of products changed from last run {0}", runIdx, runIdx - 1);

                // update previous
                previousData = new SyntheticDataSourceConfig()
                {
                    numProdPerSeller = this.config.numProdPerSeller,
                    numCustomers = this.config.numCustomers,
                    numProducts = run.numProducts,
                    qtyPerProduct = this.config.qtyPerProduct
                };
                var syntheticDataGenerator = new SyntheticDataGenerator(previousData);

                // must truncate if not first run
                if (runIdx > 0)
                    syntheticDataGenerator.TruncateTables(connection);

                syntheticDataGenerator.Generate(connection);

                await IngestionOrchestrator.Run(connection, config.ingestionConfig);
    
                if (runIdx == 0)
                {
                    // remove customers from ingestion config from now on
                    this.config.ingestionConfig.mapTableToUrl.Remove("customers");
                }

            }

            this.PreWorkload(runIdx);

            this.workloadManager.SetUp(this.config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));

            logger.LogInformation("Run #{0} started at {1}", runIdx, DateTime.UtcNow);

            var workloadTask = await this.workloadManager.Run();

            DateTime startTime = workloadTask.startTime;
            DateTime finishTime = workloadTask.finishTime;

            logger.LogInformation("Wait for microservices to converge (i.e., finish receiving events) for {0} seconds...", this.config.delayBetweenRuns / 1000);
            await Task.Delay(this.config.delayBetweenRuns);

            // set up data collection for metrics
            this.Collect(runIdx, startTime, finishTime);

            this.CollectGarbage();

            logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

            // increment run index
            runIdx++;

            this.PostRunTasks(runIdx);
        }

        logger.LogInformation("Post experiment cleanup tasks starting...");

        this.PostExperiment();

        logger.LogInformation("Experiment finished");
    }

    public async Task RunSimpleExperiment()
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.workloadManager.SetUp(this.config.runs[0].sellerDistribution, new Interval(1, this.numSellers));
        (DateTime startTime, DateTime finishTime) res = await this.workloadManager.Run();
        DateTime startTime = res.startTime;
        DateTime finishTime = res.finishTime;
        this.Collect(0, startTime, finishTime);
        this.PostRunTasks(0);
        this.PostExperiment();
        this.CollectGarbage();
    }

    protected void CollectGarbage()
    {
        logger.LogInformation("Memory used before collection:       {0:N0}",
        GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));
    }

}
