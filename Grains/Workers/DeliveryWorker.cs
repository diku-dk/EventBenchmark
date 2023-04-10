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
    /*
     * This worker models the behavior of an external provider
     * interacting with the system. 
     * TODO exact behavior and distribution of sellers
     * Some deliveries
     * 
     */
	public class DeliveryWorker : Grain, IDeliveryWorker
	{
        private string shipmentUrl;

        private IStreamProvider streamProvider;

        private IAsyncStream<int> stream;

        private readonly ILogger<DeliveryWorker> _logger;

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
            var workloadStream = streamProvider.GetStream<long>(StreamingConfiguration.DeliveryStreamId, null);
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<long>(Run);
        }

        // updating the delivery status of orders
        public async Task Run(long sellerId, StreamSequenceToken token)
        {
            // get shipped packages
            await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, shipmentUrl + "?seller_id=" + sellerId + "&status=shipped");
                HttpUtils.client.Send(message);
            });

            // TODO deserialize list of packages
            //      pick some (following a distribution?) to update
            //      send put

            return;
        }

    }
}

