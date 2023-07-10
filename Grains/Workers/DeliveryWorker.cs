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
using System.Collections.Generic;
using System.Linq;

namespace Grains.Workers
{
    [Reentrant]
    public class DeliveryWorker : Grain, IDeliveryWorker
    {
        private DeliveryWorkerConfig config;
        private IStreamProvider streamProvider;

        private readonly ILogger<DeliveryWorker> _logger;

        private long actorId;

        private readonly IDictionary<long, TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        private IAsyncStream<int> txStream;

        public DeliveryWorker(ILogger<DeliveryWorker> logger)
        {
            this._logger = logger;
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

            this.txStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        // updating the delivery status of orders
        public async Task Run(int tid, StreamSequenceToken token)
        {
            await Task.Run(() => {
                this._logger.LogInformation("Delivery {0}: Task started", this.actorId);
                try
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, config.shipmentUrl + "/" + tid);
                    this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.Now));
                    var resp = HttpUtils.client.Send(message);
                    if (resp.IsSuccessStatusCode)
                    {
                        this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));
                    }
                }
                catch(Exception e)
                {
                    this._logger.LogError("Delivery {0}: Update shipments could not be performed: {1}", this.actorId, e.Message);
                } finally
                {
                    // let emitter aware this request has finished
                    _ = txStream.OnNextAsync(tid);
                }
                this._logger.LogInformation("Delivery {0}: task terminated!", this.actorId);
            });
        }

        public Task<List<Latency>> Collect(DateTime startTime)
        {
            var targetValues = submittedTransactions.Values.Where(e => e.timestamp.CompareTo(startTime) >= 0);
            var latencyList = new List<Latency>(submittedTransactions.Count());
            foreach (var entry in targetValues)
            {
                if (finishedTransactions.ContainsKey(entry.tid)) {
                    var res = finishedTransactions[entry.tid];
                    latencyList.Add(new Latency(entry.tid, entry.type,
                        (res.timestamp - entry.timestamp).TotalMilliseconds ));
                }
            }
            return Task.FromResult(latencyList);
        }

    }
}

