using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Common.Scenario.Seller;
using Common.Streaming;
using GrainInterfaces.Scenario;
using GrainInterfaces.Workers;
using Orleans;
using Orleans.Streams;

namespace Grains.Workers
{
	public class SellerWorker : Grain, ISellerWorker
    {
        private SellerConfiguration config;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> stream;

        private long sellerId;

        public async Task Init()
        {
            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);
            this.config = await metadataService.RetrieveSellerConfig();
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.stream = streamProvider.GetStream<Event>(StreamingConfiguration.SellerReactStreamId, this.sellerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToLowStock);
                }
            }
            await stream.SubscribeAsync<Event>(ReactToLowStock);

            var workloadStream = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, this.sellerId.ToString());
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

        private async Task Run(int operation, StreamSequenceToken token)
        {
            if(operation == 0)
                UpdatePrice();
            DeleteProduct();
        }

        // driver will call
        public void DeleteProduct()
        {

        }

        // driver will call
        public void UpdatePrice()
        {

        }

        // app will send
        // low stock, marketplace offer this estimation
        // received from a continuous query (basically implement a model)[defined function or sql]
        // the standard deviation for the continuous query
        private Task ReactToLowStock(Event lowStockWarning, StreamSequenceToken token)
        {
            // given a distribution, the seller can increase and send an event increasing the stock or not
            // maybe it is reasonable to always increase stock to a minimum level
            // let's do it first
            return Task.CompletedTask;
        }
    }
}

