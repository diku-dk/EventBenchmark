using System;
using Common.Entity;
using System.Threading.Tasks;
using Orleans;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Marketplace.Infra;
using Marketplace.Message;
using Marketplace.Interfaces;
using System.Linq;

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
            this._logger.LogWarning("Product part {0} delete product ({1}) operation", this.partitionId, productId);
            this.products[productId].active = false;
            this.products[productId].updated_at = DateTime.Now.ToLongDateString();
            this._logger.LogWarning("Product part {0} finished delete product ({1}) operation", this.partitionId, productId);
            return Task.CompletedTask;
        }

        public Task<Product> GetProduct(long productId)
        {
            this._logger.LogWarning("Product part {0}, returning product {1}", this.partitionId, productId);
            return Task.FromResult(this.products[productId]);
        }

        public Task<IList<Product>> GetProducts(long sellerId)
        {
            this._logger.LogWarning("Product part {0}, returning products for seller {1}", this.partitionId, sellerId);
            return Task.FromResult( (IList<Product>) this.products.Values.Select(q => q).Where(q => q.seller_id == sellerId).ToList());
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
            this._logger.LogWarning("Product part {0} update product ({1}) price operation", this.partitionId, productId);
            // as cart actors are spread, the product actor do not know which carts are active
            // solutions: there could be a cart proxy receiving and forwarding the cart ops
            // it will be responsible for managing the  but that would lead

            // could have all carts active...
            // var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // mgmt.GetDetailedGrainStatistics(new[] { "CartActor" });
            this.products[productId].price = newPrice;
            this.products[productId].updated_at = DateTime.Now.ToLongDateString();
            this._logger.LogWarning("Product part {0} finished product ({1}) price operation", this.partitionId, productId);
            return Task.CompletedTask;
        }

        public Task<bool> AddProduct(Product product)
        {
            this._logger.LogWarning("Product part {0}, adding product ID {1}", this.partitionId, product.id);
            return Task.FromResult(this.products.TryAdd(product.id, product));
        }
    }
}

