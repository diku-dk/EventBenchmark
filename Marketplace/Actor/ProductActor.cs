using System;
using Common.Scenario.Entity;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using Marketplace.Entity;
using Marketplace.Infra;

namespace Marketplace.Actor
{

	public interface IProductActor : IGrainWithIntegerKey, SnapperActor
    {

        public Task<Product> GetProduct(long productId);

        // public Task<Product> GetProductWithFreightValue(long productId, string zipCode);

        public Task DeleteProduct(long productId);

        // seller worker calls it
        public Task UpdateProductPrice(long productId, decimal newPrice);

        public Task<ProductCheck> CheckCorrectness(BasketItem item);

        public Task<bool> AddProduct(Product product);
    }

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
        }

        public ProductActor()
		{
		}

        public Task DeleteProduct(long productId)
        {
            return Task.FromResult(products[productId].active = false);
        }

        public Task<Product> GetProduct(long productId)
        {
            return Task.FromResult(products[productId]);
        }

        public Task<ProductCheck> CheckCorrectness(BasketItem item)
        {
            var check = new ProductCheck(item.ProductId);
            if (this.products[item.ProductId].active)
            {
                if (this.products[item.ProductId].price != item.UnitPrice)
                    check.Price = this.products[item.ProductId].price;
            }
            else
            {
                check.Status = ItemStatus.DELETED;
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

            products[productId].price = newPrice;
            return Task.CompletedTask;
        }

        public Task<bool> AddProduct(Product product)
        {
            return Task.FromResult(products.TryAdd(product.product_id, product));
        }
    }
}

