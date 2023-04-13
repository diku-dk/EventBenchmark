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
        private ILogger<SellerActor> _logger;

        private Dictionary<long, Seller> sellers;
        private Dictionary<long, SortedList<long, string>> log;

        public SellerActor(ILogger<SellerActor> _logger)
        {
            this._logger = _logger;
            this.sellers = new();
            this.log = new();
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // https://github.com/dotnet/orleans/pull/1772
            // https://github.com/dotnet/orleans/issues/8262
            // GetGrain<IManagementGrain>(0).GetHosts();
            // w=>w.GrainType.Contains("PerSiloGrain")
            var stats = await mgmt.GetDetailedGrainStatistics();
            this.nProductPartitions = stats.Where(w => w.GrainType.Contains("ProductActor")).Count();
            this.nShipmentPartitions = stats.Where(w => w.GrainType.Contains("ShipmentActor")).Count();
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
            _logger.LogInformation("Product {0} deleted", productId);
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

        public Task AddSeller(Seller seller)
        {
            return Task.FromResult(this.sellers.TryAdd(seller.id, seller));
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

