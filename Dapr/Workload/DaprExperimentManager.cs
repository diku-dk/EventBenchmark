using Common.Experiment;
using Common.Infra;
using Common.Streaming;
using Common.Streaming.Redis;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Daprr.Services;
using Daprr.Workers;
using Daprr.Workrs;
using System.Text;

namespace Daprr.Workload;

public class DaprExperimentManager : ExperimentManager
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly string redisConnection;
    private readonly SellerService sellerService;
    private readonly List<string> channelsToTrim;

    private Dictionary<int, SellerThread> sellerThreads;
    private Dictionary<int, CustomerThread> customerThreads;

    private int currNumSellers;
    private DeliveryThread? deliveryThread;

    public DaprExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config) : base(config)
    {
        this.httpClientFactory = httpClientFactory;
        this.redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);

        this.sellerService = new SellerService();
        this.sellerThreads = new Dictionary<int, SellerThread>();
        this.customerThreads = new Dictionary<int, CustomerThread>();

        this.currNumSellers = 0;

        this.channelsToTrim = new();
        channelsToTrim.AddRange(config.streamingConfig.streams);

        // should also iterate over all transaction mark streams and trim them
        foreach (var type in transactions)
        {
            var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
            channelsToTrim.Add(channel);
        }
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        throw new NotImplementedException();
    }

    protected override WorkloadManager GetEmitter(int runIdx)
    {
        return new DaprWorkflowManager(
                sellerService, null, null,
                config.transactionDistribution,
                config.runs[runIdx].sellerDistribution,
                config.customerWorkerConfig.sellerRange,
                config.runs[runIdx].customerDistribution,
                customerRange,
                config.concurrencyLevel,
                config.executionTime,
                config.delayBetweenRequests);
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
        TrimStreams();

        // initialize all thread objects
        for (int i = customerRange.min; i < customerRange.max; i++)
        {
            this.customerThreads.Add(i, CustomerThread.BuildCustomerThread(httpClientFactory, sellerService, config.customerWorkerConfig, customers[i]));
        }

        this.deliveryThread = DeliveryThread.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);

    }

    /**
     * Initialize seller threads. 
     * If number not changed from last run, just reset their state.
     */
    protected override void PreWorkload()
    {
        var numSellersNext = (int)DuckDbUtils.Count(connection, "sellers");
        if(currNumSellers != numSellersNext)
        {
            currNumSellers = numSellersNext;

        } else
        {
            for (int i = 1; i < currNumSellers; i++)
            {

            }
        }

        

        
        
    }

    protected override async void TrimStreams()
    {
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);
    }
}