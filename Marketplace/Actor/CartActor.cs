using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entity;
using Marketplace.Message;
using Marketplace.Infra;
using Microsoft.Extensions.Logging;
using Orleans;
using Marketplace.Interfaces;
using Common.Event;
using System.Linq;

namespace Marketplace.Actor
{
    /**
     * Grain ID matches the customer ID
     */
    public class CartActor : Grain, ICartActor
    {
        // current basket
        private readonly Cart cart;
        private readonly IList<ProductStatus> divergences;
        private readonly Random random;

        // private readonly SortedList<long,Checkout> history;

        private long customerId;
        private readonly ILogger<CartActor> _logger;

        private int nProductPartitions;
        private int nOrderPartitions;
        private int nShipmentPartitions;
        private int nCustomerPartitions;
        private Customer customer;

        public CartActor(ILogger<CartActor> _logger)
		{
            this.cart = new Cart();
            this._logger = _logger;
            this.divergences = new List<ProductStatus>();
            this.random = new Random();
        }

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "ProductActor", "OrderActor", "ShipmentActor", "CustomerActor" });
            this.nProductPartitions = dict["ProductActor"];
            this.nOrderPartitions = dict["OrderActor"];
            this.nShipmentPartitions = dict["ShipmentActor"];
            this.nCustomerPartitions = dict["CustomerActor"];
            // get customer partition
            int custPartition = (int)(this.customerId % nCustomerPartitions);
            var custActor = GrainFactory.GetGrain<ICustomerActor>(custPartition);
            this.customer = await custActor.GetCustomer(this.customerId);
            this._logger.LogWarning("Customer loaded for cart {0}", customerId);
        }

        public Task ClearCart()
        {
            this.cart.status = CartStatus.OPEN;
            this.cart.items.Clear();
            return Task.CompletedTask;
        }

        public Task AddProduct(CartItem item)
        {

            if(item.Quantity <= 0)
            {
                throw new Exception("Item " + item.ProductId + " shows no positive value.");
            }

            if (this.cart.items.ContainsKey(item.ProductId))
            {

                if (this.cart.items[item.ProductId].Unavailable)
                {
                    this._logger.LogError("Item is unavailable. request will be discarded.", this.customerId);
                }
                else
                {
                    this._logger.LogWarning("Item already added to cart {0}. Item will be updated then.", this.customerId);
                    this.cart.items[item.ProductId] = item;
                }
                return Task.CompletedTask;
            }

            this.cart.items.Add(item.ProductId, item);
            this._logger.LogWarning("Item {0} added to cart {1}.", item.ProductId, this.customerId);
            return Task.CompletedTask;
        }

        public Task<Cart> GetCart()
        {
            this._logger.LogWarning("Cart {0} GET cart request.", this.customerId);
            return Task.FromResult(this.cart);
        }

        /*
         * TODO to discuss...
         * Indicates the customer signaling the checkout
         * Quotes the shipment of the products
         * Retrieves user payment info
         * In the future, get similar products to show to customer
         */
        public async Task SignalCheckout()
        {
            // send products to shipment, get shipment quotation
            // any shipment actor carries the quotation logic.
            // shipment partitioned by order, order is not created yet at this point
            Dictionary<long, decimal> quotationMap = await GrainFactory.GetGrain<IShipmentActor>(0).GetQuotation(this.customer.zip_code);
            // while wait, get customer info so it can be shown in the UI
            return;
        }

        // customer decided to checkout
        public async Task Checkout(CustomerCheckout basketCheckout)
        {
            this._logger.LogWarning("Cart {0} received checkout request.", this.customerId);

            if (this.cart.items.Count == 0)
                throw new Exception("Cart "+ this.customerId+" is empty.");

            if (this.customerId != basketCheckout.CustomerId)
                throw new Exception("Cart " + this.customerId + " does not correspond to customer ID received: "+basketCheckout.CustomerId);

            if (this.cart.status == Status.CHECKOUT_SENT)
                throw new Exception("Cannot checkout a cart "+ this.customerId +" that has a checkout in progress.");

            // build checkout info for order processing
            Checkout checkout = new Checkout(DateTime.Now, basketCheckout,
                this.cart.items.Select( c => c.Value).Where( c => !c.Unavailable).ToList() );

            // pick a random partition. why? (i) we do not know the order id yet (ii) distribute the work more seamlessly
            int orderPart = this.random.Next(0, this.nOrderPartitions);
            IOrderActor orderActor = this.GrainFactory.GetGrain<IOrderActor>(orderPart);
            this.cart.status = CartStatus.CHECKOUT_SENT;
            // pass the responsibility
            await orderActor.Checkout(checkout);

            Seal();

            return;
        }

        private void Seal()
        {
            if (this.cart.status == CartStatus.CHECKOUT_SENT)
            {
                this.cart.status = CartStatus.OPEN;
                this.cart.items.Clear();
            }
            else
            {
                throw new Exception("Cannot seal a cart that has not been checked out");
            }
        }

        public Task NotifyProductUnavailability(List<long> itemIds)
        {
            // mark products as unavailable
            foreach(long id in itemIds)
            {
                if(!this.cart.items.ContainsKey(id)) throw new Exception("Unavailable product has been reported to a cart that do not shows this product!");
                this.cart.items[id].Unavailable = true;
            }
            return Task.CompletedTask;
        }
    }
}

