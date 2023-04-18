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

            actorSettings.Add("OrderActor", settings.numOrderPartitions);
            actorSettings.Add("PaymentActor", settings.numPaymentPartitions);
            actorSettings.Add("ShipmentActor", settings.numShipmentPartitions);
            actorSettings.Add("CustomerActor", settings.numCustomerPartitions);
            actorSettings.Add("ProductActor", settings.numProductPartitions);
            actorSettings.Add("StockActor", settings.numStockPartitions);

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

