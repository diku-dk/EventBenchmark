using System;
using Common.Scenario.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Marketplace.Interfaces
{

    public interface IProductActor : IGrainWithIntegerKey, SnapperActor
    {

        public Task<Product> GetProduct(long productId);

        public Task<IList<Product>> GetProducts(long sellerId);

        // public Task<Product> GetProductWithFreightValue(long productId, string zipCode);

        public Task DeleteProduct(long productId);

        // seller worker calls it
        public Task UpdateProductPrice(long productId, decimal newPrice);

        public Task<ProductCheck> CheckCorrectness(BasketItem item);

        public Task<bool> AddProduct(Product product);
    }
    
}

