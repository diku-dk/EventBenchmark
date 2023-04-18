using System;
using Common.Scenario.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.State;

namespace Marketplace.Interfaces
{
    /**
     * The benchmark driver ensures a customer does not start a new cart 
     * while the current cart checkout has not been completed
     */
    public interface ICartActor : IGrainWithIntegerKey, SnapperActor
    {

        public Task AddProduct(BasketItem item);

        public Task Checkout(CustomerCheckout basketCheckout);

        public Task<CartState> GetCart();

        // public Task Seal();
    }
}

