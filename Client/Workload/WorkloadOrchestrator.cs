using Orleans;
using System.Threading.Tasks;
using Common.Workload;
using System;
using System.Collections.Generic;
using GrainInterfaces.Workers;
using Common.Streaming;
using Orleans.Streams;
using System.Collections.Concurrent;
using Common.Workload.Customer;
using Common.Infra;
using Common.Distribution.YCSB;
using Microsoft.Extensions.Logging;
using Client.Infra;
using System.Threading;
using System.Collections;
using System.Linq;
using Common.Distribution;
using Client.Streaming.Kafka;
using Confluent.Kafka;
using Client.Streaming.Redis;

namespace Client.Workload
{

    public class WorkloadOrchestrator : Stoppable
    {
        private readonly IClusterClient orleansClient;
        private readonly WorkloadConfig workloadConfig;
        private readonly Interval customerRange;

        // customer and seller workers do not need to know about other customer
        // but the transaction orchestrator needs to assign grain ids to the transactions emitted
        // public readonly Distribution customerDistribution;
        // public readonly Range customerRange;

        // provides an ID generator for each workload (e.g., customer, seller)
        // the generator obeys a distribution


        private readonly ILogger logger;

        public WorkloadOrchestrator(IClusterClient orleansClient, WorkloadConfig workloadConfig, Interval customerRange) : base()
        {
            this.orleansClient = orleansClient;
            this.workloadConfig = workloadConfig;
            this.customerRange = customerRange;
            this.logger = LoggerProxy.GetInstance("WorkloadOrchestrator");
        }

        public async Task Run()
        {
            Console.WriteLine("Workload orchestrator started.");

            WorkloadGenerator workloadGen = new WorkloadGenerator(this.workloadConfig.transactionDistribution, this.workloadConfig.concurrencyLevel);

            Task genTask = Task.Run(() => workloadGen.Run());

            // setup streams
            List<string> channels = new List<string>() { "InvoiceIssued" }; // { "PaymentConfirmed", "PaymentFailed", };

            List<(Task,CancellationTokenSource)> streamConsumers = new();
            foreach(var channel in channels)
            {
                var token = new CancellationTokenSource();
                streamConsumers.Add( (RedisConsumer.Subscribe(channel, token.Token, entry => {
                    logger.LogInformation("[RedisConsumer] Payload received from channel {0}: {1}", channel, entry.ToString());
                    Shared.ResultQueue.Add(null);
                }), token) );
            }

            WorkloadEmitter emitter = new WorkloadEmitter(orleansClient, workloadConfig.customerWorkerConfig, workloadConfig.customerDistribution, customerRange, workloadConfig.concurrencyLevel);

            Task emitTask = Task.Run(() => emitter.Run());

            await Task.WhenAny(Task.Delay(workloadConfig.executionTime));

            workloadGen.Stop();
            emitter.Stop();

            foreach(var consumer in streamConsumers)
            {
                consumer.Item2.Cancel();
            }

            /*
            List<KafkaConsumer> kafkaWorkers = new();
            if (this.workloadConfig.pubsubEnabled)
            {
                // setup kafka consumer. setup forwarding events to proper grains (e.g., customers)
                // https://github.com/YijianLiu1210/BDS-Programming-Assignment/blob/main/OrleansWorld/Client/Stream/StreamClient.cs

                logger.LogInformation("Streaming will be set up.");

                foreach (var entry in this.masterConfig.workloadConfig.mapTopicToStreamGuid)
                {
                    KafkaConsumer kafkaConsumer = new KafkaConsumer(
                        BuildKafkaConsumer(entry.Key, StreamingConfig.KafkaService),
                        this.orleansClient.GetStreamProvider(StreamingConfig.DefaultStreamProvider),
                        entry.Value,
                        entry.Key);

                    _ = Task.Run(() => kafkaConsumer.Run());
                    kafkaWorkers.Add(kafkaConsumer);
                }

                logger.LogInformation("Streaming set up finished.");
            }

            // stop kafka consumers if necessary
            if (this.masterConfig.workflowConfig.pubsubEnabled)
            {
                foreach (var task in kafkaWorkers)
                {
                    task.Stop();
                }
            }
            */


           Console.WriteLine("Workload orchestrator has finished.");

           

            return;
        }

        private static IConsumer<string, Event> BuildKafkaConsumer(string topic, string host)
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

