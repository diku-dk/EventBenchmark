using Common.Streaming;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Orleans.Concurrency;
using Common.Workload.Metrics;
using System.Collections.Concurrent;
using Common.Workload.Delivery;
using Grains.WorkerInterfaces;

namespace Grains.Workers
{
    [Reentrant]
    public class DeliveryProxy : Grain, IDeliveryProxy
    {
        private DeliveryWorkerConfig config;
        private IStreamProvider streamProvider;

        private readonly ILogger<DeliveryProxy> logger;

        private long actorId;

        private readonly IDictionary<long, TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        public DeliveryProxy(ILogger<DeliveryProxy> logger)
        {
            this.logger = logger;
            this.submittedTransactions = new ConcurrentDictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new ConcurrentDictionary<long, TransactionOutput>();
        }

        public Task Init(DeliveryWorkerConfig config)
        {
            this.config = config;
            this.submittedTransactions.Clear();
            this.finishedTransactions.Clear();
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.actorId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryWorkerNameSpace, actorId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);
        }

        // updating the delivery status of orders
        public async Task Run(int tid, StreamSequenceToken token)
        {
            this.logger.LogInformation("Delivery {0}: Task started", this.actorId);
            var worker = GrainFactory.GetGrain<IDeliveryWorker>(0);
            var res = await worker.Send(tid, config.shipmentUrl + "/" + tid);
            if (res.Item1.IsSuccessStatusCode)
            {
                this.submittedTransactions.Add(tid, res.Item2);
                this.finishedTransactions.Add(tid, res.Item3);
            }
            this.logger.LogInformation("Delivery {0}: task terminated!", this.actorId);
        }

        public Task<List<Latency>> Collect(DateTime finishTime)
        {
            var targetValues = finishedTransactions.Values.Where(e => e.timestamp.CompareTo(finishTime) <= 0);
            var latencyList = new List<Latency>(targetValues.Count());
            foreach (var entry in targetValues)
            {
                if (!submittedTransactions.ContainsKey(entry.tid))
                {
                    logger.LogWarning("Cannot find correspondent submitted TID from finished transaction {0}", entry);
                    continue;
                }
                var init = submittedTransactions[entry.tid];
                latencyList.Add(new Latency(entry.tid, init.type,
                    (entry.timestamp - init.timestamp).TotalMilliseconds, entry.timestamp));
            }
            return Task.FromResult(latencyList);
        }

    }
}

