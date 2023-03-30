using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Orleans;

namespace Marketplace.Actor
{
	public interface ICartActor : IGrainWithIntegerKey
	{
		public Task AddProduct(BasketItem product);

        public Task Checkout();
	}

    /**
     * Grain ID matches the customer ID
     */
    public class CartActor : Grain, ICartActor
    {

        private readonly List<BasketItem> items;

		public CartActor()
		{
            this.items = new List<BasketItem>();

        }

        public Task AddProduct(BasketItem product)
        {
            throw new NotImplementedException();
        }

        public Task Checkout()
        {
            throw new NotImplementedException();
            // this.Dispose();
        }
    }
}

