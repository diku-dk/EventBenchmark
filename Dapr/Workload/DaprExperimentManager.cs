using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Streaming;
using Common.Metric;
using Common.Services;
using Common.Workers;
using System.Text;
using Daprr.Streaming.Redis;
using Common.Http;
using Common.Ingestion;
using Dapr.Workers;

namespace Common.Workload;

public class DaprExperimentManager : ExperimentManager
{
    private readonly IHttpClientFactory httpClientFactory;

    private readonly string redisConnection;
    private readonly List<string> channelsToTrim;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    private readonly Dictionary<int, CustomerThread> customerThreads;
    private readonly DeliveryThread deliveryThread;

    private int numSellers;

    private DaprWorkflowManager workflowManager;
    private readonly DaprMetricManager metricManager;

    public DaprExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config) : base(config)
    {
        this.httpClientFactory = httpClientFactory;
        this.redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);

        this.deliveryThread = DeliveryThread.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        this.deliveryService = new DeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, CustomerThread>();
        this.customerService = new CustomerService(this.customerThreads);

        this.numSellers = 0;

        this.channelsToTrim = new();

        this.metricManager = new DaprMetricManager(sellerService, customerService, deliveryService);
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        string ts = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
        this.metricManager.SetUp(numSellers, config.numCustomers);
        this.metricManager.Collect(startTime, finishTime, config.epoch, string.Format("{0}#{1}_{2}_{3}_{4}_{5}", ts, runIdx, config.concurrencyLevel,
                    config.runs[runIdx].numProducts, config.runs[runIdx].sellerDistribution, config.runs[runIdx].keyDistribution));
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        this.workflowManager ??= new DaprWorkflowManager(
            sellerService, customerService, deliveryService,
            config.transactionDistribution,
            customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);
        this.workflowManager.SetUp(config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return workflowManager;
    }

    protected override async void PostExperiment()
    {
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);

        var resps = new List<Task<HttpResponseMessage>>();
        foreach (var task in config.postExperimentTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Post experiment task to URL {0}", task.url);
            resps.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps);
        logger.LogInformation("Post experiment cleanup tasks finished");
    }

    /**
     * 1. Trim streams
     * 2. Initialize all customer objects
     * 3. Initialize delivery as a single object, but multithreaded
     */
    protected override async void PreExperiment()
    {
        // cleanup microservice states
        var resps_ = new List<Task<HttpResponseMessage>>();
        foreach (var task in config.postExperimentTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Pre experiment task to URL {0}", task.url);
            resps_.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps_);

        this.channelsToTrim.AddRange(config.streamingConfig.streams);

        // should also iterate over all transaction mark streams and trim them
        foreach (var type in eventualCompletionTransactions)
        {
            var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
            this.channelsToTrim.Add(channel);
        }

        TrimStreams();

        // initialize all customer thread objects
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, CustomerThread.BuildCustomerThread(httpClientFactory, sellerService, config.numProdPerSeller, config.customerWorkerConfig, customers[i-1]));
        }

    }

    /**
     * Initialize seller threads
     */
    protected override void PreWorkload(int runIdx)
    {
        this.numSellers = (int)DuckDbUtils.Count(connection, "sellers");

        for (int i = 1; i <= numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!sellerThreads.ContainsKey(i))
            {
                sellerThreads[i] = DaprSellerThread.BuildSellerThread(i, httpClientFactory, config.sellerWorkerConfig);
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
            this.customerThreads[i].SetDistribution(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }

    }

    protected override async void TrimStreams()
    {
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);
    }

    protected override async void RunIngestion()
    {
        var ingestionOrchestrator = new IngestionOrchestrator(config.ingestionConfig);
        await ingestionOrchestrator.Run(connection);
    }

    protected override async void PostRunTasks(int runIdx, int lastRunIdx)
    {
        // reset data in microservices - post run
        if (runIdx < lastRunIdx)
        {
            logger.LogInformation("Post run tasks started");
            var responses = new List<Task<HttpResponseMessage>>();
            List<PostRunTask> postRunTasks;
            // must call the cleanup if next run changes number of products
            if (config.runs[runIdx + 1].numProducts != config.runs[runIdx].numProducts)
            {
                logger.LogInformation("Next run changes the number of products.");
                postRunTasks = config.postExperimentTasks;
            }
            else
            {
                logger.LogInformation("Next run does not change the number of products.");
                postRunTasks = config.postRunTasks;
            }
            foreach (var task in postRunTasks)
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                logger.LogInformation("Post run task to Microservice {0} URL {1}", task.name, task.url);
                responses.Add(HttpUtils.client.SendAsync(message));
            }
            await Task.WhenAll(responses);
            logger.LogInformation("Post run tasks finished");
        }

        logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

        logger.LogInformation("Memory used before collection:       {0:N0}",
                GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));

    }

}
