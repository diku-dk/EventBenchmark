using Marketplace.Infra;
using Orleans;
using System.Threading.Tasks;
using Common.Entity;
using Common.Event;
using System.Collections.Generic;

namespace Marketplace.Interfaces
{
    /**
     * The benchmark driver ensures a customer does not start a new cart 
     * while the current cart checkout has not been completed
     */
    public interface ICartActor : IGrainWithIntegerKey, SnapperActor
    {

        public Task AddProduct(CartItem item);

        public Task Checkout(CustomerCheckout basketCheckout);

        public Task<Cart> GetCart();

        public Task ClearCart();

        public Task NotifyProductUnavailability(List<long> itemIds);

        // public Task Seal();
    }
}

