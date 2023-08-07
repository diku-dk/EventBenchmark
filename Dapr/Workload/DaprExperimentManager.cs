using Common.Experiment;
using Common.Infra;
using Common.Streaming;
using Common.Streaming.Redis;
using Common.Workload;
using System.Text;

namespace Dapr.Workload;

public class DaprExperimentManager : ExperimentManager
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly string redisConnection;
    private readonly List<string> channelsToTrim;

    public DaprExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config) : base(config)
    {
        this.httpClientFactory = httpClientFactory;
        this.redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);


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

    protected override WorkloadEmitter GetEmitter(int runIdx)
    {
        return new DaprWorkflowEmitter(
                httpClientFactory,
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

    protected override void PreExperiment()
    {
        TrimStreams();
    }

    protected override void PreWorkload()
    {
        var numSellers = (int)DuckDbUtils.Count(connection, "sellers");
        // TODO clean state of all thread objects
    }

    protected override async void TrimStreams()
    {
        await RedisUtils.TrimStreams(redisConnection, channelsToTrim);
    }
}