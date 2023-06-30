using System;
using System.Threading.Tasks;
using GrainInterfaces.Workers;
using Common.Streaming;
using Orleans;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Common.Http;
using System.Net.Http;
using Orleans.Concurrency;
using Common.Workload;
using Common.Workload.Metrics;
using System.Collections.Concurrent;
using Common.Workload.Delivery;

namespace Grains.Workers
{
    [Reentrant]
    public class DeliveryWorker : Grain, IDeliveryWorker
    {
        private DeliveryWorkerConfig config;

        private IStreamProvider streamProvider;

        private readonly ILogger<DeliveryWorker> _logger;

        private long actorId;

        private readonly ConcurrentBag<TransactionIdentifier> submittedTransactions = new ConcurrentBag<TransactionIdentifier>();
        private readonly ConcurrentBag<TransactionOutput> finishedTransactions = new ConcurrentBag<TransactionOutput>();

        public DeliveryWorker(ILogger<DeliveryWorker> logger)
        {
            this._logger = logger;
        }

        public Task Init(DeliveryWorkerConfig config)
        {
            this.config = config;
            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.actorId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, actorId.ToString());
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
            await Task.Run(() => {
                this._logger.LogWarning("Delivery {0}: Task started", this.actorId);
                try
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, config.shipmentUrl + "/" + tid);
                    this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.Now));
                    var resp = HttpUtils.client.Send(message);
                    resp.EnsureSuccessStatusCode();
                    this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.Now));
                }
                catch(Exception e)
                {
                    this._logger.LogError("Delivery {0}: Update shipments could not be performed: {1}", this.actorId, e.Message);
                }
                this._logger.LogWarning("Delivery {0}: task terminated!", this.actorId);
            });
        }

    }
}

