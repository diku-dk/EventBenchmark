using System;
using Common.Scenario.Entity;
using System.Threading.Tasks;
using Orleans;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Common.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Marketplace.Interfaces;

namespace Marketplace.Actor
{

    public class ProductActor : Grain, IProductActor
    {

        private readonly Dictionary<long, Product> products;
        private long partitionId;
        private readonly ILogger<ProductActor> _logger;

        public ProductActor(ILogger<ProductActor> _logger)
        {
            this.products = new Dictionary<long, Product>();
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.partitionId = this.GetPrimaryKeyLong();
            // to avoid warning...
            await base.OnActivateAsync();
        }

        public Task DeleteProduct(long productId)
        {
            this.products[productId].active = false;
            this.products[productId].updated_at = DateTime.Now.ToLongDateString();
            return Task.CompletedTask;
        }

        public Task<Product> GetProduct(long productId)
        {
            _logger.LogWarning("Product part {0}, returning product ID {1}", this.partitionId, productId);
            return Task.FromResult(this.products[productId]);
        }

        public Task<ProductCheck> CheckCorrectness(BasketItem item)
        {
            ProductCheck check;
            if (this.products[item.ProductId].active)
            {
                if (this.products[item.ProductId].price != item.UnitPrice)
                {
                    check = new ProductCheck(item.ProductId, ItemStatus.PRICE_DIVERGENCE, this.products[item.ProductId].price);
                }
                check = new ProductCheck(item.ProductId, ItemStatus.IN_STOCK, this.products[item.ProductId].price);
            }
            else
            {
                check = new ProductCheck(item.ProductId, ItemStatus.DELETED, this.products[item.ProductId].price);
            }
            return Task.FromResult(check);
        }

        public Task UpdateProductPrice(long productId, decimal newPrice)
        {
            // as cart actors are spread, the product actor do not know which carts are active
            // solutions: there could be a cart proxy receiving and forwarding the cart ops
            // it will be responsible for managing the  but that would lead

            // could have all carts active...
            // var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // mgmt.GetDetailedGrainStatistics(new[] { "CartActor" });
            this.products[productId].price = newPrice;
            this.products[productId].updated_at = DateTime.Now.ToLongDateString();
            return Task.CompletedTask;
        }

        public Task<bool> AddProduct(Product product)
        {
            _logger.LogWarning("Product part {0}, adding product ID {1}", this.partitionId, product.id);
            return Task.FromResult(this.products.TryAdd(product.id, product));
        }
    }
}

