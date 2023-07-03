using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.DataGeneration.Real;
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
using Orleans.Streams;
using Client.Workload;
using Client.Ingestion.Config;
using Client.Streaming.Redis;
using Common.Infra;
using Common.Workload.Metrics;
using Client.Collection;

namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        public readonly WorkflowConfig workflowConfig;

        public readonly SyntheticDataSourceConfig syntheticDataConfig;

        public readonly OlistDataSourceConfiguration olistDataConfig;

        public readonly IngestionConfig ingestionConfig;

        public readonly WorkloadConfig workloadConfig;

        public readonly CollectionConfig collectionConfig;

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ILogger logger;

        public WorkflowOrchestrator(IClusterClient orleansClient, WorkflowConfig workflowConfig,
            SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig,
            WorkloadConfig workloadConfig, CollectionConfig collectionConfig)
		{
            this.orleansClient = orleansClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            this.workflowConfig = workflowConfig;
            this.syntheticDataConfig = syntheticDataConfig;
            this.ingestionConfig = ingestionConfig;
            this.workloadConfig = workloadConfig;
            this.collectionConfig = collectionConfig;

            this.logger = LoggerProxy.GetInstance("WorkflowOrchestrator");
        }

        /**
         * Initialize the first step
         */
        public async Task Run()
        {
            string redisConnection = string.Format("{0}:{1}", this.workloadConfig.streamingConfig.host, this.workloadConfig.streamingConfig.port);

            if (this.workflowConfig.healthCheck)
            {
                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
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

                // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
                // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
                if (this.workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection(redisConnection))
                {
                    logger.LogInformation("Healthcheck failed for Redis in URL {0}", redisConnection);
                }

                logger.LogInformation("Healthcheck process succeeded");
            }

            if (this.workflowConfig.dataLoad)
            {
                if(this.syntheticDataConfig != null)
                {
                    var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                    syntheticDataGenerator.Generate();
                } else {

                    if(this.olistDataConfig == null)
                    {
                        throw new Exception("Loading data is set up but no configuration was found!");
                    }

                    var realDataGenerator = new RealDataGenerator(olistDataConfig);
                    realDataGenerator.Generate();

                }
            }

            if (this.workflowConfig.ingestion)
            {
                var ingestionOrchestrator = new SimpleIngestionOrchestrator(ingestionConfig);
                ingestionOrchestrator.Run();
            }

            if (this.workflowConfig.transactionSubmission)
            {
                // get number of products
                using var connection = new DuckDBConnection(connectionString);
                connection.Open();
                var endValue = DuckDbUtils.Count(connection, "products");
                if (endValue < this.workloadConfig.customerWorkerConfig.maxNumberKeysToBrowse || endValue < this.workloadConfig.customerWorkerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
                }

                // update customer config
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                // defined dynamically
                this.workloadConfig.customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

                // activate all customer workers
                List<Task> tasks = new();
                List<Customer> customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                ICustomerWorker customerWorker = null;
                foreach (var customer in customers)
                {
                    customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                    tasks.Add( customerWorker.Init(this.workloadConfig.customerWorkerConfig, customer, this.workloadConfig.endToEndLatencyCollection, redisConnection) );
                }
                await Task.WhenAll(tasks);

                // make sure to activate all sellers so they can respond to customers when required
                // another solution is making them read from the microservice itself...
                ISellerWorker sellerWorker = null;
                tasks.Clear();
                for (int i = 1; i <= numSellers; i++)
                {
                    List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                    sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                    tasks.Add(sellerWorker.Init(this.workloadConfig.sellerWorkerConfig, products, this.workloadConfig.endToEndLatencyCollection, redisConnection));
                }
                await Task.WhenAll(tasks);

                // could be read in the data load config, but in cases the file is not read, reading from DB ensures the value is always fulfilled
                long numCustomers = DuckDbUtils.Count(connection, "customers");
                var customerRange = new Interval(1, (int)numCustomers);

                // activate delivery worker
                var deliveryWorker = this.orleansClient.GetGrain<IDeliveryWorker>(0);
                await deliveryWorker.Init(this.workloadConfig.deliveryWorkerConfig, this.workloadConfig.endToEndLatencyCollection);

                // setup transaction orchestrator
                WorkloadOrchestrator workloadOrchestrator = new WorkloadOrchestrator(this.orleansClient, this.workloadConfig, customerRange);

                var workloadTask = Task.Run(workloadOrchestrator.Run);
                DateTime startTime = workloadTask.Result.startTime;
                DateTime finishTime = workloadTask.Result.finishTime;

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(workloadTask, OrleansClientFactory._siloFailedTask.Task);

                // set up data collection for metrics
                if (this.workflowConfig.collection)
                {

                    // this is the end to end latency
                    if (this.workloadConfig.endToEndLatencyCollection) {
                        // collect() stops subscription to redis streams in every worker
                        
                        var latencyGatherTasks = new List<Task<List<Latency>>>();

                        foreach (var customer in customers)
                        {
                            customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                            latencyGatherTasks.Add( customerWorker.Collect(startTime) );
                        }

                        for (int i = 1; i <= numSellers; i++)
                        {
                            sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                            latencyGatherTasks.Add(sellerWorker.Collect(startTime));
                        }

                        latencyGatherTasks.Add( deliveryWorker.Collect(startTime) );


                        // this does not mean the system has finished processing it. we need to get the last tid processed from the grains
                        // var lastTidSubmitted = Shared.Workload.Take().tid - 1;
                        // TODO continue...

                    }

                    // check whether prometheus is online
                    string urlMetric = collectionConfig.baseUrl + "/" + collectionConfig.ready;
                    logger.LogInformation("Contacting {0} metric collection API healthcheck on {1}", urlMetric);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, urlMetric);
                    var resp = HttpUtils.client.Send(message);
                    if (resp.IsSuccessStatusCode)
                    {

                        // collect data from prometheus here
                        // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_ingress_count
                        // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count&name=ReserveInventory
                        // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart"}&time=1688136844
                        // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart",topic="ReserveStock"}&time=1688136844

                        var ingressCountPerMs = new Dictionary<string,long>();

                        foreach(var entry in collectionConfig.egress_topics)
                        {

                        }

                        var egressCountPerMs = new Dictionary<string, long>();


                    }
                    else
                    {
                        logger.LogError("It was not possible to contact {0} metric collection API healthcheck on {1}", urlMetric);
                    }

                  
                }

            }

            if (this.workflowConfig.cleanup)
            {
                // DuckDbUtils.DeleteAll(connection, "products", "seller_id = " + i);
            }

            return;

        }

    }
}