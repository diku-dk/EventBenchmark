using Orleans;
using System.Threading.Tasks;
using Common.Workload;
using System.Collections.Generic;
using Common.Infra;
using Microsoft.Extensions.Logging;
using System.Linq;
using Client.Streaming.Redis;

namespace Client.Workload
{

    public class WorkloadOrchestrator
    {
        private readonly IClusterClient orleansClient;
        private readonly WorkloadConfig workloadConfig;
        private readonly Interval customerRange;
        private readonly Interval deliveryRange;
        private readonly ILogger logger;

        public WorkloadOrchestrator(IClusterClient orleansClient, WorkloadConfig workloadConfig, Interval customerRange, Interval deliveryRange)
        {
            this.orleansClient = orleansClient;
            this.workloadConfig = workloadConfig;
            this.customerRange = customerRange;
            this.deliveryRange = deliveryRange;
            this.logger = LoggerProxy.GetInstance("WorkloadOrchestrator");
        }

        public async Task Run()
        {
            logger.LogInformation("Workload orchestrator started.");

            // clean streams beforehand. make sure microservices do not receive events from previous runs
            List<string> channelsToTrim = workloadConfig.streamingConfig.streams.ToList();

            Task trimTasks = Task.Run(() => RedisUtils.TrimStreams(channelsToTrim));

            WorkloadGenerator workloadGen = new WorkloadGenerator(this.workloadConfig.transactionDistribution, this.workloadConfig.concurrencyLevel);

            Task genTask = Task.Run(workloadGen.Run);

            await trimTasks;

            WorkloadEmitter emitter = new WorkloadEmitter(
                orleansClient,
                workloadConfig.customerWorkerConfig.sellerDistribution,
                workloadConfig.customerWorkerConfig.sellerRange,
                workloadConfig.customerDistribution,
                customerRange,
                deliveryRange,
                workloadConfig.concurrencyLevel);

            Task emitTask = Task.Run(emitter.Run);

            await Task.WhenAny(Task.Delay(workloadConfig.executionTime));

            workloadGen.Stop();
            emitter.Stop();

            logger.LogInformation("Workload orchestrator has finished.");

        }

    }
}

