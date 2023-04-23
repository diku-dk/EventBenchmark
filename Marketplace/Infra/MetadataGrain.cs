using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace Marketplace.Infra
{
    public class MetadataGrain : Grain, IMetadataGrain
    {
        private IDictionary<string, int> actorSettings;
        private ActorSettings settings;

        public MetadataGrain()
        {
            this.actorSettings = new Dictionary<string,int>();
        }

        public Task Init(ActorSettings settings)
        {
            this.settings = settings;

            actorSettings.TryAdd("OrderActor", settings.numOrderPartitions);
            actorSettings.TryAdd("PaymentActor", settings.numPaymentPartitions);
            actorSettings.TryAdd("ShipmentActor", settings.numShipmentPartitions);
            actorSettings.TryAdd("CustomerActor", settings.numCustomerPartitions);
            actorSettings.TryAdd("ProductActor", settings.numProductPartitions);
            actorSettings.TryAdd("StockActor", settings.numStockPartitions);

            return Task.CompletedTask;
        }

        public Task<IDictionary<string, int>> GetActorSettings(IList<string> actors)
        {
            IDictionary<string,int> resp = new Dictionary<string, int>();
            foreach(var actor in actors)
            {
                resp.Add(actor, actorSettings[actor]);
            }
            return Task.FromResult(resp);
        }

        
    }
}

