using System;
using Common.Configuration;
using Common.Scenario.Customer;
using Common.YCSB;
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

namespace Grains.Workers
{
	public class DeliveryWorker : Grain, IDeliveryWorker
	{
        private string shipmentUrl;

        private IStreamProvider streamProvider;

        private IAsyncStream<int> stream;

        private readonly ILogger<CustomerWorker> _logger;

        // updating the delivery status of orders

        public DeliveryWorker(ILogger<CustomerWorker> logger)
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
            var workloadStream = streamProvider.GetStream<int>(StreamingConfiguration.DeliveryStreamId, null);
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

        public async Task Run(int op, StreamSequenceToken token)
        {
            // get open orders
            // per open order
            // get number of deliveries
            // get random num and close them (some may still be open)
            // actually this must be performed by the microservice logic itself
            // take a look at tpc-c
            await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, shipmentUrl + "/update");
                HttpUtils.client.Send(message);
            });
            return;
        }

    }
}

