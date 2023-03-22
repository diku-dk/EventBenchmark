using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.Execution;
using Client.Infra;
using Client.Ingestion;
using Client.Streaming.Kafka;
using Common.Http;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Scenario.Customer;
using Common.Streaming;
using Confluent.Kafka;
using GrainInterfaces.Ingestion;
using GrainInterfaces.Scenario;
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

        // streams
        private readonly IStreamProvider streamProvider;

        // synchronization with possible many ingestion orchestrator
        // CountdownEvent ingestionProcess;

        public MasterOrchestrator(MasterConfiguration masterConfig)
		{
            this.masterConfig = masterConfig;
            this.streamProvider = masterConfig.orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
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

            if (masterConfig.load)
            {
                if(masterConfig.syntheticConfig != null)
                {

                    var syntheticDataGenerator = new SyntheticDataGenerator(masterConfig.syntheticConfig);
                    syntheticDataGenerator.Generate();

                } else {

                    if(masterConfig.olistConfig == null)
                    {
                        throw new Exception("Loading data is set up but no configuration was found!");
                    }



                }
            }

            if (masterConfig.healthCheck)
            {
                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
                var responses = new List<Task<HttpResponseMessage>>();
                foreach (var tableUrl in masterConfig.ingestionConfig.mapTableToUrl)
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, tableUrl.Value);
                    responses.Add(HttpUtils.client.SendAsync(message));
                }

                await Task.WhenAll(responses);

                int idx = 0;
                foreach (var tableUrl in masterConfig.ingestionConfig.mapTableToUrl)
                {
                    if (!responses[idx].Result.IsSuccessStatusCode)
                    {
                        Console.WriteLine("Healthcheck failed for {0}", tableUrl.Value);
                        return;
                    }
                    idx++;
                }

            }

            if (masterConfig.ingestion)
            {

                var ingestionOrchestrator = new IngestionOrchestrator(masterConfig.ingestionConfig);

                ingestionOrchestrator.Run();

                /*
                IIngestionOrchestrator ingestionOrchestrator = masterConfig.orleansClient.GetGrain<IIngestionOrchestrator>(0);

                // make sure is online to receive stream
                await ingestionOrchestrator.Init(ingestionConfig);

                Console.WriteLine("Ingestion orchestrator grain will start.");

                IAsyncStream<int> ingestionStream = streamProvider.GetStream<int>(StreamingConfiguration.IngestionStreamId, 0.ToString());

                this.ingestionProcess = new CountdownEvent(1);

                IAsyncStream<int> resultStream = streamProvider.GetStream<int>(StreamingConfiguration.IngestionStreamId, "master");

                var subscription = await resultStream.SubscribeAsync(FinalizeIngestion);

                await ingestionStream.OnNextAsync(0);

                ingestionProcess.Wait();

                await subscription.UnsubscribeAsync();

                Console.WriteLine("Ingestion orchestrator grain finished.");
                */
            }

            if (masterConfig.transaction)
            {

                masterConfig.scenarioConfig.customerConfig.urls = masterConfig.ingestionConfig.mapTableToUrl;
                CustomerConfiguration customerConfig = masterConfig.scenarioConfig.customerConfig;

                if (!customerConfig.urls.ContainsKey("products"))
                {
                    //Console.WriteLine("Customer {0} found no products URL!", this.customerId);
                    return;
                }
                if (!customerConfig.urls.ContainsKey("carts"))
                {
                    //Console.WriteLine("Customer {0} found no carts URL!", this.customerId);
                    return;
                }

                var endValue = customerConfig.keyRange.End.Value;
                if (endValue < customerConfig.maxNumberKeysToBrowse || endValue < customerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("That may lead to customer grain looping forever!");
                }

                // register customer config
                IMetadataService metadataService = masterConfig.orleansClient.GetGrain<IMetadataService>(0);
                metadataService.RegisterCustomerConfig(customerConfig);

                List<KafkaConsumer> kafkaTasks = new();
                if (this.masterConfig.streamEnabled)
                {
                    // setup kafka consumer. setup forwarding events to proper grains (e.g., customers)
                    // https://github.com/YijianLiu1210/BDS-Programming-Assignment/blob/main/OrleansWorld/Client/Stream/StreamClient.cs

                    Console.WriteLine("Streaming will be set up.");

                    foreach (var entry in masterConfig.scenarioConfig.mapTopicToStreamGuid)
                    {
                        KafkaConsumer kafkaConsumer = new KafkaConsumer(
                            BuildKafkaConsumer(entry.Key, StreamingConfiguration.KafkaService),
                            masterConfig.orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider),
                            entry.Value,
                            entry.Key);

                        _ = Task.Run(() => kafkaConsumer.Run());
                        kafkaTasks.Add(kafkaConsumer);
                    }

                    Console.WriteLine("Streaming set up finished.");
                }

                // setup transaction orchestrator
                TransactionOrchestrator transactionOrchestrator = new TransactionOrchestrator(masterConfig.orleansClient, masterConfig.scenarioConfig);

                _ = Task.Run(() => transactionOrchestrator.Run());

                Thread.Sleep(masterConfig.scenarioConfig.period);

                transactionOrchestrator.Stop();

                // stop kafka consumers if necessary
                if (this.masterConfig.streamEnabled)
                {
                    foreach (var task in kafkaTasks)
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

