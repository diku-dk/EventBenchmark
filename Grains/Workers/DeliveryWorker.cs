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
using Common.Scenario.Entity;
using Newtonsoft.Json;
using Common.Entity;
using System.Collections.Generic;

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
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
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

        // updating the delivery status of orders
        public async Task Run(int op, StreamSequenceToken token)
        {
            // move this later to seller worker => get overview
            /*
            // get open orders (not delivered status)

            // get shipped packages
            HttpResponseMessage response = await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get,
                    shipmentUrl + "?seller_id=" + sellerId +
                    "&status=shipped,sort=shipment_id+asc");
                return HttpUtils.client.Send(message);
            });

            var packagesStr = await response.Content.ReadAsStringAsync();
            List<Package> packages = JsonConvert.DeserializeObject<List<Package>>(packagesStr);

            
            //   TODO   pick some (following a distribution?) to update
            //      send put
            */

            HttpResponseMessage response = await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch,
                    shipmentUrl + "/update");
                return HttpUtils.client.Send(message);
            });

            return;
        }

    }
}

