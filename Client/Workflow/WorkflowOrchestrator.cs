using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.Infra;
using Client.Ingestion;
using Common.Http;
using Common.Workload;
using Common.Entities;
using Common.Streaming;
using DuckDB.NET.Data;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Orleans;
using Client.Workload;
using Client.Ingestion.Config;
using Client.Streaming.Redis;
using Common.Infra;
using Client.Collection;
using System.Linq;
using Client.Cleaning;

namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        private readonly WorkflowConfig workflowConfig;

        private readonly SyntheticDataSourceConfig syntheticDataConfig;

        private readonly IngestionConfig ingestionConfig;

        private readonly WorkloadConfig workloadConfig;

        private readonly CollectionConfig collectionConfig;

        private readonly CleaningConfig cleaningConfig;

        private readonly ILogger logger;

        public WorkflowOrchestrator(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig,
            WorkloadConfig workloadConfig, CollectionConfig collectionConfig, CleaningConfig cleaningConfig)
		{
            this.workflowConfig = workflowConfig;
            this.syntheticDataConfig = syntheticDataConfig;
            this.ingestionConfig = ingestionConfig;
            this.workloadConfig = workloadConfig;
            this.collectionConfig = collectionConfig;
            this.cleaningConfig = cleaningConfig;
            this.logger = LoggerProxy.GetInstance("WorkflowOrchestrator");
        }

        /**
         * Initialize the first step
         */
        public async Task Run()
        {

            if (this.workflowConfig.healthCheck)
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
                string redisConn = string.Format("{0}:{1}", this.workloadConfig.streamingConfig.host, this.workloadConfig.streamingConfig.port);
                // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
                // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
                if (this.workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection(redisConn))
                {
                    logger.LogInformation("Healthcheck failed for Redis in URL {0}", redisConn);
                }

                logger.LogInformation("Healthcheck process succeeded");
            }

            if (this.workflowConfig.dataLoad)
            {
                var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                syntheticDataGenerator.Generate();
            }

            if (this.workflowConfig.ingestion)
            {
                var ingestionOrchestrator = new SimpleIngestionOrchestrator(ingestionConfig);
                ingestionOrchestrator.Run();
            }

            if (this.workflowConfig.transactionSubmission)
            {

                logger.LogInformation("Initializing Orleans client...");
                var orleansClient = await OrleansClientFactory.Connect();
                if (orleansClient == null) {
                    logger.LogError("Error on contacting Orleans Silo.");
                    return;
                }
                logger.LogInformation("Orleans client initialized!");

                var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

                // get number of products
                using var connection = new DuckDBConnection(this.workloadConfig.connectionString);
                connection.Open();
                List<Customer> customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                var customerRange = new Interval(1, customers.Count());

                // initialize all workers
                await Prepare(orleansClient, customers, numSellers, connection);

                // setup transaction orchestrator
                WorkloadOrchestrator workloadOrchestrator = new WorkloadOrchestrator(orleansClient, this.workloadConfig, customerRange);

                var workloadTask = Task.Run(workloadOrchestrator.Run);
                DateTime startTime = workloadTask.Result.startTime;
                DateTime finishTime = workloadTask.Result.finishTime;

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(workloadTask, OrleansClientFactory._siloFailedTask.Task);

                // set up data collection for metrics
                if (this.workflowConfig.collection)
                {
                    MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, this.collectionConfig);
                    await metricGather.Collect(startTime, finishTime);
                }

                await orleansClient.Close();
                logger.LogInformation("Orleans client finalized!");
            }

            if (this.workflowConfig.cleanup)
            {
                // clean streams beforehand. make sure microservices do not receive events from previous runs
                List<string> channelsToTrim = cleaningConfig.streamingConfig.streams.ToList();
                string redisConnection = string.Format("{0}:{1}", this.cleaningConfig.streamingConfig.host, this.cleaningConfig.streamingConfig.port);
                logger.LogInformation("Triggering {0} stream cleanup on {1}", cleaningConfig.streamingConfig.type, redisConnection);
                Task trimTasks = Task.Run(() => RedisUtils.TrimStreams(redisConnection, channelsToTrim));
                await trimTasks; // should also iterate over all transaction mark streams and trim them

                List<Task> responses = new();
                // truncate duckdb tables
                foreach(var entry in this.cleaningConfig.mapMicroserviceToUrl)
                {
                    var urlCleanup = entry.Value + CleaningConfig.cleanupEndpoint;
                    logger.LogInformation("Triggering {0} cleanup on {1}", entry.Key, urlCleanup);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, urlCleanup);
                    responses.Add(HttpUtils.client.SendAsync(message));
                }
                await Task.WhenAll(responses);
                // DuckDbUtils.DeleteAll(connection, "products", "seller_id = " + i);
            }

            return;

        }

        public async Task Prepare(IClusterClient orleansClient, List<Customer> customers, long numSellers, DuckDBConnection connection)
        {
            var endValue = DuckDbUtils.Count(connection, "products");
            if (endValue < workloadConfig.customerWorkerConfig.maxNumberKeysToBrowse || endValue < workloadConfig.customerWorkerConfig.maxNumberKeysToAddToCart)
            {
                throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
            }

            // defined dynamically
            workloadConfig.customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

            string redisConnection = string.Format("{0}:{1}", workloadConfig.streamingConfig.host, workloadConfig.streamingConfig.port);

            // activate all customer workers
            List<Task> tasks = new();

            ICustomerWorker customerWorker = null;
            foreach (var customer in customers)
            {
                customerWorker = orleansClient.GetGrain<ICustomerWorker>(customer.id);
                tasks.Add(customerWorker.Init(workloadConfig.customerWorkerConfig, customer, redisConnection));
            }
            await Task.WhenAll(tasks);

            // make sure to activate all sellers so they can respond to customers when required
            // another solution is making them read from the microservice itself...
            ISellerWorker sellerWorker = null;
            tasks.Clear();
            for (int i = 1; i <= numSellers; i++)
            {
                List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                sellerWorker = orleansClient.GetGrain<ISellerWorker>(i);
                tasks.Add(sellerWorker.Init(workloadConfig.sellerWorkerConfig, products, redisConnection));
            }
            await Task.WhenAll(tasks);

            // activate delivery worker
            var deliveryWorker = orleansClient.GetGrain<IDeliveryWorker>(0);
            await deliveryWorker.Init(workloadConfig.deliveryWorkerConfig);

        }

    }
}