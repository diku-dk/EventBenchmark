using Orleans;
using System.Threading.Tasks;
using Common.Workload;
using Common.Infra;
using Microsoft.Extensions.Logging;
using System;
using Common.Distribution;
using System.Collections.Generic;

namespace Client.Workload
{
    public class WorkloadOrchestrator
    {
        private readonly static ILogger logger = LoggerProxy.GetInstance("WorkloadOrchestrator");

        public static async Task<(DateTime startTime, DateTime finishTime)> Run(IClusterClient orleansClient, IDictionary<TransactionType, int> transactionDistribution, int concurrencyLevel,
            DistributionType sellerDistribution, Interval sellerRange, DistributionType customerDistribution, Interval customerRange, int delayBetweenRequests, int executionTime)
        {
            logger.LogInformation("Workload orchestrator started.");

            WorkloadGenerator workloadGen = new WorkloadGenerator(
                transactionDistribution, concurrencyLevel);

            // makes sure there are transactions
            workloadGen.Prepare();

            Task genTask = Task.Factory.StartNew(workloadGen.Run, TaskCreationOptions.PreferFairness);

            WorkloadEmitter emitter = new WorkloadEmitter(
                orleansClient,
                sellerDistribution,
                sellerRange,
                customerDistribution,
                customerRange,
                concurrencyLevel,
                executionTime,
                delayBetweenRequests);

            Task<(DateTime startTime, DateTime finishTime)> emitTask =
                Task.Run(emitter.Run);
            // var emitTask = await Task.Factory.StartNew(emitter.Run, TaskCreationOptions.LongRunning);

            // await Task.Delay(executionTime);
            await emitTask;

            workloadGen.Stop();
            // to make sure the generator leaves the loop
            Shared.WaitHandle.Add(0);

            // await Task.WhenAll(genTask, emitTask);

            logger.LogInformation("Workload orchestrator has finished.");

            // return emitter.GetStartAndFinishTime();
            return emitTask.Result;
        }

    }
}

