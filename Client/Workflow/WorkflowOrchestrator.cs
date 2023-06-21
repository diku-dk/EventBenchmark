using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.DataGeneration.Real;
using Client.Infra;
using Client.Ingestion;
using Client.Streaming.Kafka;
using Common.Http;
using Common.Ingestion;
using Common.Workload;
using Common.Workload.Customer;
using Common.Entities;
using Common.Workload.Seller;
using Common.Streaming;
using Confluent.Kafka;
using DuckDB.NET.Data;
using GrainInterfaces.Ingestion;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Client.Workload;
using static Confluent.Kafka.ConfigPropertyNames;
using Client.Ingestion.Config;
using Client.Streaming.Redis;

namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        public WorkflowConfig workflowConfig = null;

        public SyntheticDataSourceConfig syntheticDataConfig = null;

        public OlistDataSourceConfiguration olistDataConfig = null;

        public IngestionConfig ingestionConfig = null;

        public WorkloadConfig workloadConfig;

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ILogger logger;

        // synchronization with possible many ingestion orchestrator
        // CountdownEvent ingestionProcess;

        public WorkflowOrchestrator(IClusterClient orleansClient, WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig, WorkloadConfig workloadConfig)
		{
            this.orleansClient = orleansClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            this.workflowConfig = workflowConfig;
            this.syntheticDataConfig = syntheticDataConfig;
            this.ingestionConfig = ingestionConfig;
            this.workloadConfig = workloadConfig;

            this.logger = LoggerProxy.GetInstance("WorkflowOrchestrator");
        }

        /*
        private Task FinalizeIngestion(int obj, StreamSequenceToken token = null)
        {
            if (this.ingestionProcess == null) throw new Exception("Semaphore not initialized properly!");
            this.ingestionProcess.Signal();
            return Task.CompletedTask;
        }
        */

        /**
         * Initialize the first step
         */
        public async Task Run()
        {
            if (this.workflowConfig.healthCheck)
            {
                string healthCheckEndpoint = this.workflowConfig.healthCheckEndpoint;
                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
                var responses = new List<Task<HttpResponseMessage>>();
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    var urlHealth = tableUrl.Value + healthCheckEndpoint;
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
                        logger.LogInformation("Healthcheck failed for {0} in URL {1}", tableUrl.Key, tableUrl.Value);
                        return;
                    }
                    idx++;
                }

                // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
                // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
                if (this.workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection())
                {
                    logger.LogInformation("Healthcheck failed for Redis in URL {0}", "localhost");
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

                /*
                IIngestionOrchestrator ingestionOrchestrator = masterConfig.orleansClient.GetGrain<IIngestionOrchestrator>(0);

                // make sure is online to receive stream
                await ingestionOrchestrator.Init(ingestionConfig);

                logger.LogInformation("Ingestion orchestrator grain will start.");

                IAsyncStream<int> ingestionStream = streamProvider.GetStream<int>(StreamingConfiguration.IngestionStreamId, 0.ToString());

                this.ingestionProcess = new CountdownEvent(1);

                IAsyncStream<int> resultStream = streamProvider.GetStream<int>(StreamingConfiguration.IngestionStreamId, "master");

                var subscription = await resultStream.SubscribeAsync(FinalizeIngestion);

                await ingestionStream.OnNextAsync(0);

                ingestionProcess.Wait();

                await subscription.UnsubscribeAsync();

                logger.LogInformation("Ingestion orchestrator grain finished.");
                */
            }

            if (this.workflowConfig.transactionSubmission)
            {
                // customer session distriburtion 100 in 0 in transaction submssion
                this.workloadConfig.customerWorkerConfig.urls = workloadConfig.mapTableToUrl;
                CustomerWorkerConfig customerConfig = workloadConfig.customerWorkerConfig;

                if (!customerConfig.urls.ContainsKey("products"))
                {
                    throw new Exception("No products URL found! Execution suspended.");
                }
                if (!customerConfig.urls.ContainsKey("carts"))
                {
                    throw new Exception("No carts URL found! Execution suspended.");
                }
                if (!customerConfig.urls.ContainsKey("customers"))
                {
                    throw new Exception("No customers URL found! Execution suspended.");
                }

                // get number of products
                using var connection = new DuckDBConnection(connectionString);
                connection.Open();
                var endValue = DuckDbUtils.Count(connection, "products");
                if (endValue < customerConfig.maxNumberKeysToBrowse || endValue < customerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
                }

                // update customer config
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                // defined dynamically
                this.workloadConfig.customerWorkerConfig.sellerRange = new Interval(1, (int) numSellers);

                // make sure to activate all sellers so all can respond to customers when required
                // another solution is making them read from the microservice itself...
                ISellerWorker sellerWorker = null;
                List<Task> tasks = new();
                for (int i = 1; i <= numSellers; i++)
                {
                    List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                    sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                    tasks.Add( sellerWorker.Init(this.workloadConfig.sellerWorkerConfig, products) );
                }
                await Task.WhenAll(tasks);

                // activate delivery worker
                await this.orleansClient.GetGrain<IDeliveryWorker>(0).Init(this.workloadConfig.mapTableToUrl["shipments"]);

                // could be read in the data load config, but in cases the file is not read, reading from DB ensures the value is always fulfilled
                long numCustomers = DuckDbUtils.Count(connection, "customers");
                var customerRange = new Interval(1, (int)numCustomers);

                // setup transaction orchestrator
                WorkloadOrchestrator workloadOrchestrator = new WorkloadOrchestrator(this.orleansClient, this.workloadConfig, customerRange);

                Task workloadTask = Task.Run(() => workloadOrchestrator.Run());

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(workloadTask, OrleansClientFactory._siloFailedTask.Task);               

            }

            if (this.workflowConfig.collection)
            {

                // set up data collection for metrics


            }

            return;

        }

    }
}