using System;
using Common.Scenario.Entity;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using Marketplace.Entity;

namespace Marketplace.Actor
{

	public interface IProductActor : IGrainWithIntegerKey {

        public Task<Product> GetProduct(long productId);

        // public Task<Product> GetProductWithFreightValue(long productId, string zipCode);

        public Task DeleteProduct(long productId);

        // seller worker calls it
        public Task UpdateProductPrice(long productId, decimal newPrice);

        public Task<ProductCheck> CheckCorrectness(BasketItem item);
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
            throw new NotImplementedException();
        }

        public Task<Product> GetProduct(long productId)
        {
            throw new NotImplementedException();
        }

        public Task<ProductCheck> CheckCorrectness(BasketItem item)
        {
            var check = new ProductCheck(item.ProductId);
            if (products.ContainsKey(item.ProductId))
            {
                if (products[item.ProductId].price != item.UnitPrice)
                    check.Price = products[item.ProductId].price;
            }
            else
            {
                check.Status = ItemStatus.DELETED;
            }
            return Task.FromResult(check);
        }

        public Task UpdateProductPrice(long productId, decimal newPrice)
        {
            throw new NotImplementedException();
            // as cart actors are spread, the product actor do not know which carts are active
            // solutions: there could be a cart proxy receiving and forwarding the cart ops
            // it will be responsible for managing the  but that would lead

            // could have all carts active...
            // var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // mgmt.GetDetailedGrainStatistics(new[] { "CartActor" });
        }
    }
}

