using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Workload;
using Common.Services;
using Common.Workers.Seller;
using Microsoft.Extensions.Logging;
using Orleans.Workers;
using Common.Workers.Customer;
using Common.Http;
using Orleans.Metric;
using DuckDB.NET.Data;
using Common.Workers.Delivery;

namespace Orleans.Workload;

public sealed class ActorExperimentManager : AbstractExperimentManager
{
    private readonly IHttpClientFactory httpClientFactory;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    private readonly Dictionary<int, AbstractCustomerWorker> customerThreads;
    private readonly DefaultDeliveryWorker deliveryThread;

    private int numSellers;

    private readonly ActorWorkloadManager workloadManager;
    private readonly ActorMetricManager metricManager;

    public ActorExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection) : base(config, connection)
    {
        this.httpClientFactory = httpClientFactory;

        this.deliveryThread = DefaultDeliveryWorker.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        this.deliveryService = new DeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, AbstractCustomerWorker>();
        this.customerService = new CustomerService(this.customerThreads);

        this.numSellers = 0;

        this.workloadManager = new ActorWorkloadManager(
            sellerService, customerService, deliveryService,
            config.transactionDistribution,
            // set in the base class
            this.customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);

        this.metricManager = new ActorMetricManager(sellerService, customerService, deliveryService);
    }

    public async Task RunSimpleExperiment(int type)
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
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

    protected override void PreExperiment()
    {
        Console.WriteLine("Initializing customer threads...");
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, ActorCustomerWorker.BuildCustomerThread(this.httpClientFactory, this.sellerService, this.config.numProdPerSeller, this.config.customerWorkerConfig, this.customers[i-1]));
        }
    }

    protected override void PreWorkload(int runIdx)
    {
        Console.WriteLine("Initializing seller threads...");
        this.numSellers = (int)DuckDbUtils.Count(this.connection, "sellers");
        for (int i = 1; i <= this.numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!this.sellerThreads.ContainsKey(i))
            {
                this.sellerThreads[i] = ActorSellerWorker.BuildSellerThread(i, this.httpClientFactory, this.config.sellerWorkerConfig);
                this.sellerThreads[i].SetUp(products, this.config.runs[runIdx].keyDistribution);
            }
            else
            {
                this.sellerThreads[i].SetUp(products, this.config.runs[runIdx].keyDistribution);
            }
        }

        Console.WriteLine("Setting up seller workload info in customer threads...");
        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads[i].SetUp(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        this.workloadManager.SetUp(this.config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return this.workloadManager;
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        this.metricManager.SetUp(this.numSellers, this.config.numCustomers);
        this.metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}_{6}", ts, runIdx, this.config.numCustomers, this.config.concurrencyLevel, this.config.runs[runIdx].numProducts, this.config.runs[runIdx].sellerDistribution, this.config.runs[runIdx].keyDistribution));
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

    protected override async void PostExperiment()
    {
        await this.TriggerPostExperimentTasks();
    }

    protected override async void PostRunTasks(int runIdx, int lastRunIdx)
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

}
