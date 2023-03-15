using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Dynamic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.Infra;
using Client.Streaming.Kafka;
using Common.Entities.TPC_C;
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
using static Confluent.Kafka.ConfigPropertyNames;

namespace Client
{
	public class MasterOrchestrator
	{

        private readonly MasterConfiguration masterConfig;
        private readonly IngestionConfiguration ingestionConfig;
        private readonly ScenarioConfiguration scenarioConfiguration;

        // orleans client
        private readonly IClusterClient clusterClient;

        // streams
        private readonly IStreamProvider streamProvider;

        //
        CountdownEvent ingestionProcess;

        public MasterOrchestrator(
            MasterConfiguration masterConfig,
            IngestionConfiguration ingestionConfig,
            ScenarioConfiguration scenarioConfig)
		{
            this.masterConfig = masterConfig;
            this.ingestionConfig = ingestionConfig;
            this.scenarioConfiguration = scenarioConfig;

            this.clusterClient = masterConfig.orleansClient;
            this.streamProvider = clusterClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
        }

        private Task FinalizeIngestion(object obj, StreamSequenceToken token = null)
        {
            if (this.ingestionProcess == null) throw new Exception("Semaphore not initialized properly!");
            this.ingestionProcess.Signal();
            return Task.CompletedTask;
        }

        /**
         * Initialize the first step
         */
        public async void Run()
		{

            if (masterConfig.healthCheck)
            {
                // for each table and associated url, perform a GET request to check if return is OK
                // health check. is the microservice online?
                var responses = new List<Task<HttpResponseMessage>>();
                foreach(var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, tableUrl.Value + "/1");
                    responses.Add( HttpUtils.client.SendAsync(message) );
                }

                await Task.WhenAll(responses);

                int idx = 0;
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    // Console.WriteLine("Health check status code: " + response.StatusCode.ToString());
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

                IIngestionOrchestrator ingestionOrchestrator = masterConfig.orleansClient.GetGrain<IIngestionOrchestrator>(0);

                // make sure is online to receive stream
                await ingestionOrchestrator.Init(ingestionConfig);

                Console.WriteLine("Ingestion orchestrator grain will start.");

                var ingestionStream = streamProvider.GetStream<object>(StreamingConfiguration.IngestionStreamId, "0");

                // _ = ingestionOrchestrator.Run(ingestionConfig);
                await ingestionStream.OnNextAsync(new object());

                var resultStream = streamProvider.GetStream<object>(StreamingConfiguration.IngestionStreamId, "master");

                this.ingestionProcess = new CountdownEvent(1);

                await ingestionStream.SubscribeAsync(FinalizeIngestion);

                // can be made simpler with orleans streams
                //var status = 0;
                //while (status == 0)
                //{
                //    Thread.Sleep(2000);
                //    status = await ingestionOrchestrator.GetStatus();
                //}

                ingestionProcess.Wait();

                Console.WriteLine("Ingestion orchestrator grain finished.");
            }

            if (masterConfig.transactionSubmission)
            {

                // set customer config
                CustomerConfiguration customerConfig = new()
                {
                    maxNumberKeysToBrowse = 10,
                    keyDistribution = Common.Configuration.Distribution.UNIFORM,
                    // keyRange = new Range(1, TpccConstants.NUM_I + 1),
                    keyRange = new Range(1, 15),
                    urls = ingestionConfig.mapTableToUrl,
                    minMaxQtyRange = new Range(1, 11),
                    maxNumberKeysToAddToCart = 10,
                    delayBetweenRequestsRange = new Range(1, 1000),
                    delayBeforeStart = 0
                };

                var endValue = customerConfig.keyRange.End.Value;
                if (endValue < customerConfig.maxNumberKeysToBrowse || endValue < customerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("That may lead to customer grain looping forever!");
                }

                IMetadataService metadataService = masterConfig.orleansClient.GetGrain<IMetadataService>(0);
                metadataService.RegisterCustomerConfig(customerConfig);

                if (this.masterConfig.streamEnabled)
                {
                    // setup kafka consumer. setup forwarding events to proper grains (e.g., customers)
                    // https://github.com/YijianLiu1210/BDS-Programming-Assignment/blob/main/OrleansWorld/Client/Stream/StreamClient.cs

                    Console.WriteLine("Streaming will be set up.");

                    foreach (var entry in scenarioConfiguration.mapTopicToStreamGuid)
                    {
                        KafkaConsumer kafkaConsumer = new KafkaConsumer(
                            BuildKafkaConsumer(entry.Key, StreamingConfiguration.KafkaService),
                            masterConfig.orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider),
                            entry.Value,
                            entry.Key);

                        Task kafkaConsumerTask = Task.Run(() => kafkaConsumer.Run());
                    }

                    Console.WriteLine("Streaming set up finished.");
                }

                // setup transaction orchestrator
                IScenarioOrchestrator scenarioOrchestrator = masterConfig.orleansClient.GetGrain<IScenarioOrchestrator>(0);

                // FIXME  await end of submission of transactions
                // setup clock here instead of inisde the scenario orchestrator
                _ = scenarioOrchestrator.Start(scenarioConfiguration);
                // var watch = new Stopwatch();
                Thread.Sleep(scenarioConfiguration.period);

                await scenarioOrchestrator.Stop();
            }

            // set up data collection for metrics

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

