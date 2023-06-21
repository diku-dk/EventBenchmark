using System;
using Common.Workload.Customer;
using Common.Distribution.YCSB;
using System.Threading.Tasks;
using GrainInterfaces.Workers;
using Common.Streaming;
using Orleans;
using System.IO;
using Microsoft.Extensions.Logging;
using Orleans.Streams;
using Common.Http;
using System.Net.Http;
using System.Text.RegularExpressions;
using Common.Entities;
using Newtonsoft.Json;
using System.Collections.Generic;
using Orleans.Concurrency;

namespace Grains.Workers
{
    [Reentrant]
    public class DeliveryWorker : Grain, IDeliveryWorker
	{
        private string shipmentUrl;

        private IStreamProvider streamProvider;

        private readonly ILogger<DeliveryWorker> _logger;

        private long actorId;

        private IAsyncStream<int> txStream;

        public DeliveryWorker(ILogger<DeliveryWorker> logger)
        {
            this._logger = logger;
        }

        public Task Init(string shipmentUrl)
        {
            this.shipmentUrl = shipmentUrl;
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
            this._logger.LogWarning("Delivery {0}: Task started", this.actorId);

            try
            {
                HttpResponseMessage response = await Task.Run(() =>
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch,
                        shipmentUrl + "/" + tid);
                    return HttpUtils.client.Send(message);
                });
            }
            catch(Exception e)
            {
                this._logger.LogError("Delivery {0}: Update shipments could not be performed: {1}", this.actorId, e.Message);
            }
            finally
            {
                // let emitter aware this request has finished
                _ = txStream.OnNextAsync(tid);
            }

            this._logger.LogWarning("Delivery {0}: task terminated!", this.actorId);

            return;
        }

    }
}

