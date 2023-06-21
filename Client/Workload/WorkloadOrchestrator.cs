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

    public class WorkloadOrchestrator
    {
        private readonly IClusterClient orleansClient;
        private readonly WorkloadConfig workloadConfig;
        private readonly Interval customerRange;
        private readonly ILogger logger;

        public WorkloadOrchestrator(IClusterClient orleansClient, WorkloadConfig workloadConfig, Interval customerRange)
        {
            this.orleansClient = orleansClient;
            this.workloadConfig = workloadConfig;
            this.customerRange = customerRange;
            this.logger = LoggerProxy.GetInstance("WorkloadOrchestrator");
        }

        public async Task Run()
        {
            Console.WriteLine("Workload orchestrator started.");

            // clean streams beforehand. make sure microservices do not receive events from previous runs
            List<string> channelsToTrim = workloadConfig.streamingConfig.streams.ToList();

            Task trimTasks = Task.Run(()=> RedisUtils.TrimStreams(channelsToTrim) );

            WorkloadGenerator workloadGen = new WorkloadGenerator(this.workloadConfig.transactionDistribution, this.workloadConfig.concurrencyLevel);

            Task genTask = Task.Run(workloadGen.Run);

            // setup streams to listen to
            List<string> channelsToListen = workloadConfig.streamingConfig.txStreams.ToList();

            List<(Task,CancellationTokenSource)> streamConsumers = new();
            foreach(var channel in channelsToListen)
            {
                var token = new CancellationTokenSource();
                // should it be a thread per consumer? https://stackoverflow.com/questions/37419572
                streamConsumers.Add( (RedisUtils.Subscribe(channel, token.Token, entry => {
                    logger.LogInformation("[RedisConsumer] Payload received from channel {0}: {1}", channel, entry.ToString());
                    Shared.ResultQueue.Add(null);
                }), token ) );
            }

            await trimTasks;

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

