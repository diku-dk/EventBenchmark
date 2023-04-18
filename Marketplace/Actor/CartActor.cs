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

            /*
            // customer decided to checkout even with the divergences presented earlier
            if(this.status == Status.PRODUCT_DIVERGENCE)
            {
                foreach(var item in divergences.Where(p => p.Status == ItemStatus.UNAVAILABLE || p.Status == ItemStatus.OUT_OF_STOCK))
                {
                    items.Remove(item.Id);
                }
            }
            */

            // should check correctness of all products? no, stick to the benchmark
            // this does not give strong guarantees anyway
            /*
            List<Task<ProductCheck>> tasks = new();
            int prodPart;
            foreach(var item in items)
            {
                prodPart = (int)(item.Key % nProductPartitions);
                tasks.Add(GrainFactory.GetGrain<IProductActor>(prodPart).CheckCorrectness(item.Value));
            }

            await Task.WhenAll(tasks);

            foreach(var resp in tasks)
            {

            }
            */

            // build checkout info for order processing
            Checkout checkout = new(DateTime.Now, basketCheckout, this.state.items);
            // int orderPart = (int)(this.customerId % nOrderPartitions);
            // pick a random partition. why? (i) we do not know the order id yet (ii) distribute the work more seamlessly
            int orderPart = this.random.Next(0, nOrderPartitions);
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(orderPart);
            this.state.status = Status.CHECKOUT_SENT;
            // pass the responsibility
            await orderActor.Checkout(checkout);

            Seal();
            /*
            if (resp.inconsistencies.Count() > 0)
            {
                this.status = Status.PRODUCT_DIVERGENCE;
                divergences.Clear();
                divergences.AddRange(resp.inconsistencies);

                // update basket items
                foreach (var item in divergences)
                {
                    if (item.Status == ItemStatus.UNAVAILABLE || item.Status == ItemStatus.OUT_OF_STOCK)
                    {
                        items[item.Id].unavailable = true;
                        continue;
                    }

                    items[item.Id].OldUnitPrice = items[item.Id].UnitPrice;
                    items[item.Id].UnitPrice = item.Price;

                }
            }
            else
            {
                this.status = Status.OPEN;
                this.items.Clear();
            }
            */
            return;
        }

        /**
         * Only used in a pure event-based model.
         * In Snapper, everything is synchronous, so cannot be used to eventually close a cart...
         */
        private void Seal()
        {
            if (this.state.status == Status.CHECKOUT_SENT)
            {
                this.state.status = Status.OPEN;
                this.state.items.Clear();
                // history.Add()
                // return Task.CompletedTask;
            }
            else
            {
                throw new Exception("Cannot seal a cart that has not been checked out");
            }
        }

    }
}

