using System;
using System.Dynamic;
using System.Threading;
using System.Threading.Tasks;
using Client.Infra;
using Client.Streaming.Kafka;
using Common.Entities.TPC_C;
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
using static Confluent.Kafka.ConfigPropertyNames;

namespace Client
{
	public class MasterOrchestrator
	{

        private readonly MasterConfiguration masterConfig;
        private readonly IngestionConfiguration ingestionConfig;
        private readonly ScenarioConfiguration scenarioConfiguration;

        public MasterOrchestrator(
            MasterConfiguration masterConfig,
            IngestionConfiguration ingestionConfig,
            ScenarioConfiguration scenarioConfig)
		{
            this.masterConfig = masterConfig;
            this.ingestionConfig = ingestionConfig;
            this.scenarioConfiguration = scenarioConfig;
        }

		public async Task Run()
		{
            IIngestionOrchestrator ingestionOrchestrator = masterConfig.orleansClient.GetGrain<IIngestionOrchestrator>(0);


            Console.WriteLine("Ingestion orchestrator grain will start.");

            _ = ingestionOrchestrator.Run(ingestionConfig);

            // can be made simpler with orleans streams
            var status = 0;
            while(status == 0)
            {
                Thread.Sleep(2000);
                status = await ingestionOrchestrator.GetStatus();
            }

            Console.WriteLine("Ingestion orchestrator grain finished.");

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
                delayBeforeStart = 0,
                streamProvider = StreamingConfiguration.defaultStreamProvider
            };

            var endValue = customerConfig.keyRange.End.Value;
            if (endValue < customerConfig.maxNumberKeysToBrowse || endValue < customerConfig.maxNumberKeysToAddToCart)
            {
                throw new Exception("That will lead to a bug!");
            }

            IMetadataService metadataService = masterConfig.orleansClient.GetGrain<IMetadataService>(0);
            metadataService.RegisterCustomerConfig(customerConfig);

            if (this.masterConfig.streamEnabled) {
                // setup kafka consumer. setup forwarding events to proper grains (e.g., customers)
                // https://github.com/YijianLiu1210/BDS-Programming-Assignment/blob/main/OrleansWorld/Client/Stream/StreamClient.cs

                Console.WriteLine("Streaming will be set up.");

                foreach (var entry in scenarioConfiguration.mapTopicToStreamGuid)
                {
                    KafkaConsumer kafkaConsumer = new KafkaConsumer(
                        BuildKafkaConsumer( entry.Key, StreamingConfiguration.kafkaService ),
                        masterConfig.orleansClient.GetStreamProvider(StreamingConfiguration.defaultStreamProvider),
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

            Thread.Sleep(scenarioConfiguration.period);

            await scenarioOrchestrator.Stop();

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

