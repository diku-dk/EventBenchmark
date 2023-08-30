using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Streaming;
using Common.Streaming.Redis;
using Common.Workload;
using Dapr.Metric;
using Daprr.Services;
using Daprr.Workers;
using Daprr.Workrs;
using System.Text;

namespace Daprr.Workload;

public class DaprExperimentManager : ExperimentManager
{
    private readonly IHttpClientFactory httpClientFactory;

    private readonly string redisConnection;
    private readonly List<string> channelsToTrim;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, SellerThread> sellerThreads;
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

        this.sellerThreads = new Dictionary<int, SellerThread>();
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

    protected override void PostExperiment()
    {
        TrimStreams();
    }

    /**
     * 1. Trim streams
     * 2. Initialize all customer objects
     * 3. Initialize delivery as a single object, but multithreaded
     */
    protected override void PreExperiment()
    {
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
                sellerThreads[i] = SellerThread.BuildSellerThread(i, httpClientFactory, config.sellerWorkerConfig);
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

}
