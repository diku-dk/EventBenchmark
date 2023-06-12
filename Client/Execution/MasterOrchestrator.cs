using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.DataGeneration.Real;
using Client.Execution;
using Client.Infra;
using Client.Ingestion;
using Client.Streaming.Kafka;
using Common.Http;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Scenario.Customer;
using Common.Entities;
using Common.Scenario.Seller;
using Common.Streaming;
using Confluent.Kafka;
using DuckDB.NET.Data;
using GrainInterfaces.Ingestion;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Transaction;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Client
{
	public class MasterOrchestrator
	{

        private readonly MasterConfiguration masterConfig;

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ILogger logger;

        // synchronization with possible many ingestion orchestrator
        // CountdownEvent ingestionProcess;

        public MasterOrchestrator(IClusterClient orleansClient, MasterConfiguration masterConfig, ILogger logger)
		{
            this.orleansClient = orleansClient;
            this.masterConfig = masterConfig;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.logger = logger;
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
            if (this.masterConfig.workflowConfig.healthCheck)
            {
                string healthCheckEndpoint = this.masterConfig.workflowConfig.healthCheckEndpoint;
                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
                var responses = new List<Task<HttpResponseMessage>>();
                foreach (var tableUrl in masterConfig.ingestionConfig.mapTableToUrl)
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
                foreach (var tableUrl in masterConfig.ingestionConfig.mapTableToUrl)
                {
                    if (!responses[idx].Result.IsSuccessStatusCode)
                    {
                        logger.LogInformation("Healthcheck failed for {0} in URL {1}", tableUrl.Key, tableUrl.Value);
                        return;
                    }
                    idx++;
                }
                logger.LogInformation("Healthcheck process succeeded");
            }

            if (this.masterConfig.workflowConfig.dataLoad)
            {
                if(this.masterConfig.syntheticDataConfig != null)
                {
                    var syntheticDataGenerator = new SyntheticDataGenerator(masterConfig.syntheticDataConfig);
                    syntheticDataGenerator.Generate();
                } else {

                    if(this.masterConfig.olistDataConfig == null)
                    {
                        throw new Exception("Loading data is set up but no configuration was found!");
                    }

                    var realDataGenerator = new RealDataGenerator(masterConfig.olistDataConfig);
                    realDataGenerator.Generate();

                }
            }

            if (this.masterConfig.workflowConfig.ingestion)
            {

                var ingestionOrchestrator = new IngestionOrchestrator(masterConfig.ingestionConfig);

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

            if (this.masterConfig.workflowConfig.transactionSubmission)
            {
                // customer session distriburtion 100 in 0 in transaction submssion
                this.masterConfig.scenarioConfig.customerWorkerConfig.urls = masterConfig.scenarioConfig.mapTableToUrl;
                CustomerWorkerConfig customerConfig = masterConfig.scenarioConfig.customerWorkerConfig;

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
                using var connection = new DuckDBConnection(masterConfig.connectionString);
                connection.Open();
                var endValue = DuckDbUtils.Count(connection, "products");
                if (endValue < customerConfig.maxNumberKeysToBrowse || endValue < customerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
                }

                // update customer config
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                // defined dynamically
                this.masterConfig.scenarioConfig.customerWorkerConfig.sellerRange = new Interval(1, (int) numSellers);

                // make sure to activate all sellers so all can respond to customers when required
                // another solution is making them read from the microservice itself...
                ISellerWorker sellerWorker = null;
                List<Task> tasks = new();
                for (int i = 1; i <= numSellers; i++)
                {
                    List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                    sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                    tasks.Add( sellerWorker.Init(this.masterConfig.scenarioConfig.sellerWorkerConfig, products) );
                }
                await Task.WhenAll(tasks);

                // activate delivery worker
                await this.orleansClient.GetGrain<IDeliveryWorker>(0).Init(this.masterConfig.scenarioConfig.mapTableToUrl["shipments"]);

                List<KafkaConsumer> kafkaWorkers = new();
                if (this.masterConfig.workflowConfig.pubsubEnabled)
                {
                    // setup kafka consumer. setup forwarding events to proper grains (e.g., customers)
                    // https://github.com/YijianLiu1210/BDS-Programming-Assignment/blob/main/OrleansWorld/Client/Stream/StreamClient.cs

                    logger.LogInformation("Streaming will be set up.");

                    foreach (var entry in this.masterConfig.scenarioConfig.mapTopicToStreamGuid)
                    {
                        KafkaConsumer kafkaConsumer = new KafkaConsumer(
                            BuildKafkaConsumer(entry.Key, StreamingConfiguration.KafkaService),
                            this.orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider),
                            entry.Value,
                            entry.Key);

                        _ = Task.Run(() => kafkaConsumer.Run());
                        kafkaWorkers.Add(kafkaConsumer);
                    }

                    logger.LogInformation("Streaming set up finished.");
                }

                // defined dynamically
                long numCustomers = DuckDbUtils.Count(connection, "customers");
                var customerRange = new Interval(1, (int)numCustomers);

                // setup transaction orchestrator
                TransactionOrchestrator transactionOrchestrator = new TransactionOrchestrator(
                    this.orleansClient, this.masterConfig.scenarioConfig, customerRange);

                _ = Task.Run(() => transactionOrchestrator.Run());

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(Task.Delay(this.masterConfig.scenarioConfig.executionTime), OrleansClientFactory._siloFailedTask.Task);

                transactionOrchestrator.Stop();
                
                // stop kafka consumers if necessary
                if (this.masterConfig.workflowConfig.pubsubEnabled)
                {
                    foreach (var task in kafkaWorkers)
                    {
                        task.Stop();
                    }
                }

                // set up data collection for metrics
                return;

            }

        }

        private static IConsumer<string,Event> BuildKafkaConsumer(string topic, string host)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = host,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                GroupId = "driver"
            };

            var consumerBuilder = new ConsumerBuilder<string, Event>(config)
                .SetKeyDeserializer(new EventDeserializer())
                .SetValueDeserializer(new PayloadDeserializer());

            IConsumer<string, Event> consumer = consumerBuilder.Build();
            return consumer;
        }

    }
}

