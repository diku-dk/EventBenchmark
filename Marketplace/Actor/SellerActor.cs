using System;
using Common.Scenario.Entity;
using Marketplace.Entity;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;
using System.Linq;

namespace Marketplace.Actor
{

    public interface ISellerActor : IGrainWithIntegerKey
    {
        [AlwaysInterleave]
        public Task DeleteProduct(long productId);

        public Task UpdatePrices(List<Product> products);
    }

    public class SellerActor : Grain, ISellerActor
    {
        private long sellerId;
        private int nProductPartitions;
        private ILogger<SellerActor> _logger;

        public SellerActor(ILogger<SellerActor> _logger)
        {
            
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // https://github.com/dotnet/orleans/pull/1772
            // https://github.com/dotnet/orleans/issues/8262
            // GetGrain<IManagementGrain>(0).GetHosts();
            // w=>w.GrainType.Contains("PerSiloGrain")
            var stats = await mgmt.GetDetailedGrainStatistics(new[] { "ProductActor" });
            this.nProductPartitions = stats.Length;
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
                prodPart = (int)(item.product_id % nProductPartitions);
                tasks.Add(GrainFactory.GetGrain<IProductActor>(prodPart).UpdateProductPrice(item.product_id, item.price));
            }

            await Task.WhenAll(tasks);

            // not handling errors in snapper-base code
            /*
            if(tasks.Where(t => !t.IsCompletedSuccessfully).Count() > 0)
            {

            }
            */


        }
    }
}

