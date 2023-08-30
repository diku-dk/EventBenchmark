using Common.DataGeneration;
using Common.Infra;
using Common.Ingestion;
using Common.Http;
using Common.Workload;
using Common.Entities;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Common.Collection;
using Common.Cleaning;
using Common.Ingestion.Config;
using Common.Streaming;
using System.Text;

namespace Common.Workflow;

public class WorkflowOrchestrator
{

    private static readonly ILogger logger = LoggerProxy.GetInstance("WorkflowOrchestrator");

    /**
    * Single run
    */

    /*
    public async static Task Run(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig,
        WorkloadConfig workloadConfig, CollectionConfig collectionConfig, CleaningConfig cleaningConfig)
    {

        if (workflowConfig.healthCheck)
        {
            var responses = new List<Task<HttpResponseMessage>>();
            foreach (var tableUrl in ingestionConfig.mapTableToUrl)
            {
                var urlHealth = tableUrl.Value + WorkflowConfig.healthCheckEndpoint;
                logger.LogInformation("Contacting {0} healthcheck on {1}", tableUrl.Key, urlHealth);
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, urlHealth);
                responses.Add(HttpUtils.client.SendAsync(message));
            }

            try
            {
                await Task.WhenAll(responses);
            } catch(Exception e)
            {
                logger.LogError("Error on contacting healthcheck: {0}", e.Message);
                return;
            }

            int idx = 0;
            foreach (var tableUrl in ingestionConfig.mapTableToUrl)
            {
                if (!responses[idx].Result.IsSuccessStatusCode)
                {
                    logger.LogError("Healthcheck failed for {0} in URL {1}", tableUrl.Key, tableUrl.Value);
                    return;
                }
                idx++;
            }

            logger.LogInformation("Healthcheck succeeded for all URLs {1}", ingestionConfig.mapTableToUrl);
            string redisConn = string.Format("{0}:{1}", workloadConfig.streamingConfig.host, workloadConfig.streamingConfig.port);
            // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
            // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
            if (workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection(redisConn))
            {
                logger.LogInformation("Healthcheck failed for Redis in URL {0}", redisConn);
            }

            logger.LogInformation("Healthcheck process succeeded");
        }

        if (workflowConfig.dataLoad)
        {
            using (DuckDBConnection connection = new DuckDBConnection(syntheticDataConfig.connectionString))
            {
                var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                connection.Open();
                syntheticDataGenerator.CreateSchema(connection);
                syntheticDataGenerator.Generate(connection, true);
            }
        }

        if (workflowConfig.ingestion)
        {
            using (DuckDBConnection connection = new DuckDBConnection(ingestionConfig.connectionString))
            {
                var ingestionOrchestrator = new IngestionOrchestrator(ingestionConfig);
                connection.Open();
                await ingestionOrchestrator.Run(connection);
            }
        }

        if (workflowConfig.transactionSubmission)
        {

            logger.LogInformation("Initializing Orleans client...");
            var orleansClient = await OrleansClientFactory.Connect();
            if (orleansClient == null) {
                logger.LogError("Error on contacting Orleans Silo.");
                return;
            }
            logger.LogInformation("Orleans client initialized!");

            var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            // get number of sellers
            using var connection = new DuckDBConnection(workloadConfig.connectionString);
            connection.Open();
            List<Customer> customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
            var numSellers = (int) DuckDbUtils.Count(connection, "sellers");
            var customerRange = new Interval(1, customers.Count());

            // initialize all workers
            await PrepareWorkers(orleansClient, workloadConfig.transactionDistribution, workloadConfig.customerWorkerConfig, workloadConfig.sellerWorkerConfig,
                workloadConfig.deliveryWorkerConfig, customers, numSellers, connection);

            // eventual completion transactions
            string redisConnection = string.Format("{0}:{1}", workloadConfig.streamingConfig.host, workloadConfig.streamingConfig.port);

            List<CancellationTokenSource> tokens = new(3);
            List<Task> listeningTasks = new(3);
            foreach (var type in transactions)
            {
                var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                var token = new CancellationTokenSource();
                tokens.Add(token);
                listeningTasks.Add(SubscribeToRedisStream(redisConnection, channel, token));
            }

            // setup transaction orchestrator
            WorkloadEmitter emitter = new WorkloadEmitter( orleansClient, workloadConfig.transactionDistribution, workloadConfig.customerWorkerConfig.sellerDistribution,
                workloadConfig.customerWorkerConfig.sellerRange, workloadConfig.customerDistribution, customerRange,
                workloadConfig.concurrencyLevel, workloadConfig.executionTime, workloadConfig.delayBetweenRequests);

            Task<(DateTime startTime, DateTime finishTime)> emitTask = Task.Run(emitter.Run);

            // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
            await Task.WhenAny(emitTask, OrleansClientFactory._siloFailedTask.Task);

            DateTime startTime = emitTask.Result.startTime;
            DateTime finishTime = emitTask.Result.finishTime;

            foreach (var token in tokens)
            {
                token.Cancel();
            }

            await Task.WhenAll(listeningTasks);

            // set up data collection for metrics
            if (workflowConfig.collection)
            {
                MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, collectionConfig);
                await metricGather.Collect(startTime, finishTime);
            }
        }

        if (workflowConfig.cleanup)
        {
            await Clean(cleaningConfig);
        }

        return;

    }
    */

    private static async Task Clean(CleaningConfig cleaningConfig)
    {
        // await RedisUtils.TrimStreams(cleaningConfig.streamingConfig.host, cleaningConfig.streamingConfig.streams.ToList());

        List<Task> responses = new();
        // truncate duckdb tables
        foreach (var entry in cleaningConfig.mapMicroserviceToUrl)
        {
            var urlCleanup = entry.Value + CleaningConfig.cleanupEndpoint;
            logger.LogInformation("Triggering {0} cleanup on {1}", entry.Key, urlCleanup);
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, urlCleanup);
            responses.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(responses);
    }

}
