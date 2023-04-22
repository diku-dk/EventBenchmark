using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Marketplace.Message;
using Marketplace.Infra;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;
using Marketplace.Interfaces;
using Common.State;

namespace Marketplace.Actor
{
    /**
     * Grain ID matches the customer ID
     */
    public class CartActor : Grain, ICartActor
    {
        // current basket
        private readonly CartState state;
        private readonly IList<ProductCheck> divergences;
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
            this.state = new CartState();
            this._logger = _logger;
            this.divergences = new List<ProductCheck>();
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
            _logger.LogWarning("Customer loaded for cart {0}", customerId);
        }

        public Task AddProduct(BasketItem item)
        {
            if (this.state.items.ContainsKey(item.ProductId))
            {
                this._logger.LogWarning("Item already added to cart {0}. Item will be updated then.", customerId);
                this.state.items[item.ProductId] = item;
                return Task.CompletedTask;
            }
            this.state.items.Add(item.ProductId, item);
            this._logger.LogWarning("Item {0} added to cart {1}", item.ProductId, customerId);
            return Task.CompletedTask;
        }

        public Task<CartState> GetCart()
        {
            this._logger.LogWarning("Cart {0} GET cart request.", this.customerId);
            return Task.FromResult(this.state);
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
            Dictionary<long, decimal> quotationMap = await GrainFactory.GetGrain<IShipmentActor>(0).GetQuotation(this.customer.zip_code_prefix);
            // while wait, get customer info so it can be shown in the UI
            return;
        }

        // customer decided to checkout
        public async Task Checkout(CustomerCheckout basketCheckout)
        {
            this._logger.LogWarning("Cart {0} received checkout request.", this.customerId);

            if (this.state.items.Count == 0)
                throw new Exception("Cart "+ this.customerId+" is empty.");

            if(this.state.status == Status.CHECKOUT_SENT)
                throw new Exception("Cannot checkout a cart "+ customerId+" that has a checkout in progress.");

            // build checkout info for order processing
            Checkout checkout = new(DateTime.Now, basketCheckout, this.state.items);

            // pick a random partition. why? (i) we do not know the order id yet (ii) distribute the work more seamlessly
            int orderPart = this.random.Next(0, nOrderPartitions);
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(orderPart);
            this.state.status = Status.CHECKOUT_SENT;
            // pass the responsibility
            await orderActor.Checkout(checkout);

            Seal();
            return;
        }

        private void Seal()
        {
            if (this.state.status == Status.CHECKOUT_SENT)
            {
                this.state.status = Status.OPEN;
                this.state.items.Clear();
            }
            else
            {
                throw new Exception("Cannot seal a cart that has not been checked out");
            }
        }

    }
}

