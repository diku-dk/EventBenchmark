using System;
using Common.Scenario.Entity;
using System.Threading.Tasks;
using Orleans;

namespace Marketplace.Actor
{

	public interface IProductActor : IGrainWithIntegerKey {

        public Task<Product> GetProduct(long productId);

        public Task DeleteProduct(long productId);

        // seller worker calls it
        public Task UpdateProductPrice(long productId, decimal newPrice);
    }


    public class ProductActor : Grain, IProductActor
    {
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

        public Task UpdateProductPrice(long productId, decimal newPrice)
        {
            throw new NotImplementedException();
        }
    }
}

