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

        private int nProductPartitions;
        private int nShipmentPartitions;

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

        public Task<Seller> GetSeller()
        {
            return Task.FromResult(this.seller);
        }

        public async Task<IList<Product>> GetProducts()
        {
            // lack of actor indexing... must call all actors...
            List<Product> products = new();
            List<Task<IList<Product>>> tasks = new(nProductPartitions);

            for(int i = 0; i < nProductPartitions; i++)
            {
                tasks.Add(GrainFactory.GetGrain<IProductActor>(i).GetProducts(this.sellerId));

            }

            await Task.WhenAll(tasks);

            for (int i = 0; i < nProductPartitions; i++)
            {
                products.AddRange(tasks[i].Result);
            }

            return products;
        }

        /**
         * Snapper has no notion of declared FK across microservices/actors
         * FK must be enforced by the code itself
         */
        public async Task DeleteProduct(long productId)
        {
            this._logger.LogWarning("Seller {0} starting delete product ({1}) operation", this.sellerId, productId);
            // stock partition is the same of product
            int prodPart = (int)(productId % nProductPartitions);
            Task[] tasks = new Task[2];
            // send first to stock
            tasks[0] = GrainFactory.GetGrain<IStockActor>(prodPart).DeleteItem(productId);
            // maintain the integrity
            tasks[1] = GrainFactory.GetGrain<IProductActor>(prodPart).DeleteProduct(productId);
            await Task.WhenAll(tasks);
            this._logger.LogWarning("Seller {0} product {0} deleted", this.sellerId, productId);
        }

        public async Task UpdatePrices(IList<Product> products)
        {
            this._logger.LogWarning("Seller {0} starting update product prices operation", this.sellerId);
            List<Task> tasks = new();
            int prodPart;
            foreach (var item in products)
            {
                prodPart = (int)(item.id % nProductPartitions);
                tasks.Add(GrainFactory.GetGrain<IProductActor>(prodPart).UpdateProductPrice(item.id, item.price));
            }

            await Task.WhenAll(tasks);
            this._logger.LogWarning("Seller {0} finished update product prices operation", this.sellerId);
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

    }
}

