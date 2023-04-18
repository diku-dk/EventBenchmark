using System;
using Common.Scenario.Entity;
using Common.Entity;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using System.Linq;
using Marketplace.Infra;
using Marketplace.Interfaces;

namespace Marketplace.Actor
{

    public class SellerActor : Grain, ISellerActor
    {
        private long sellerId;
        private long nProductPartitions;
        private long nShipmentPartitions;
        private readonly ILogger<SellerActor> _logger;

        private Seller seller;
        private readonly SortedList<long, string> log;

        public SellerActor(ILogger<SellerActor> _logger)
        {
            this._logger = _logger;
            this.log = new();
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "ProductActor", "ShipmentActor" });
            this.nProductPartitions = dict["ProductActor"];
            this.nShipmentPartitions = dict["ShipmentActor"];
        }

        public Task Init(Seller seller)
        {
            this.seller = seller;
            return Task.CompletedTask;
        }

        /**
         * Snapper has no notion of declared FK across microservices/actors
         * FK must be enforced by the code itself
         */
        public async Task DeleteProduct(long productId)
        {
            // stock partition is the same of product
            int prodPart = (int)(productId % nProductPartitions);
            Task[] tasks = new Task[2];
            // send first to stock
            tasks[0] = GrainFactory.GetGrain<IStockActor>(prodPart).DeleteItem(productId);
            // maintain the integrity
            tasks[1] = GrainFactory.GetGrain<IProductActor>(prodPart).DeleteProduct(productId);
            await Task.WhenAll(tasks);
            _logger.LogWarning("Product {0} deleted", productId);
        }

        public async Task UpdatePrices(List<Product> products)
        {
            List<Task> tasks = new();
            int prodPart;
            foreach (var item in products)
            {
                prodPart = (int)(item.id % nProductPartitions);
                tasks.Add(GrainFactory.GetGrain<IProductActor>(prodPart).UpdateProductPrice(item.id, item.price));
            }

            await Task.WhenAll(tasks);

        }

        /**
         * Seller "glues" together product and stock
         * TODO should this be the goto approach?
         */
        public async Task IncreaseStock(long productId, int quantity)
        {
            var prodPart = (productId % nProductPartitions);
            Product product = await GrainFactory.GetGrain<IProductActor>(prodPart).GetProduct(productId);
            if (product.active)
            {
               var res = await GrainFactory.GetGrain<IStockActor>(prodPart).IncreaseStock(productId, quantity);
               // if(res.Item1 == ItemStatus.OUT_OF_STOCK && res.Item2 == ItemStatus.IN_STOCK)
            } else
            {
                await GrainFactory.GetGrain<IStockActor>(prodPart).noOp();
            }
            // TODO log result for the seller
            return;
        }

        public Task<Seller> GetSeller()
        {
            return Task.FromResult(this.seller);  
        }
    }
}

