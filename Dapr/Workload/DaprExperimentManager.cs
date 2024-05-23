using Common.Experiment;
using Common.Streaming;
using Common.Metric;
using System.Text;
using Daprr.Metric;
using Daprr.Streaming.Redis;
using Common.Http;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workers.Customer;
using DuckDB.NET.Data;
using Common.Workers.Delivery;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Daprr.Workload;

public class DaprExperimentManager : AbstractExperimentManager
{

    private readonly string redisConnection;
    private readonly List<string> channelsToTrim;

    new private readonly DaprMetricManager metricManager;

    protected static readonly List<TransactionType> eventualCompletionTransactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.UPDATE_PRODUCT };

    public static DaprExperimentManager BuildDaprExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection)
    {
        return new DaprExperimentManager(httpClientFactory, DefaultSellerWorker.BuildSellerWorker, DefaultCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private DaprExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection connection) :
        base(httpClientFactory, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, connection)
    {
        this.redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);
        this.channelsToTrim = new();
        this.metricManager = new DaprMetricManager(sellerService, customerService, deliveryService);
    }

    protected override async void PostExperiment()
    {
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);
        base.PostExperiment();
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

        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);

        base.PreExperiment();

    }

    protected override async void PostRunTasks(int runIdx)
    {
        // trim first to avoid receiving events after the post run task
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);

        // reset data in microservices - post run
        if (runIdx < this.config.runs.Count - 1)
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

    }

}
