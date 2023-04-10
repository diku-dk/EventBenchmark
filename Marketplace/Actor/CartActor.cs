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

namespace Marketplace.Actor
{
    /**
     * The benchmark driver ensures a customer does not start a new cart 
     * while the current cart checkout has not been completed
     */
	public interface ICartActor : IGrainWithIntegerKey, SnapperActor
    {
		public Task AddProduct(BasketItem item);

        public Task<Invoice> Checkout(CustomerCheckout basketCheckout);

        public Task<Dictionary<long, BasketItem>> GetCart();

        // public Task Seal();
	}

    public enum Status
    {
        OPEN,
        CHECKOUT_SENT,
        PRODUCT_DIVERGENCE
    };

    /**
     * Grain ID matches the customer ID
     */
    public class CartActor : Grain, ICartActor
    {
        // current basket
        private Status status;
        private readonly Dictionary<long, BasketItem> items;
        public readonly List<ProductCheck> divergences;
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
            this.status = Status.OPEN;
            this.items = new Dictionary<long,BasketItem>();
            this._logger = _logger;
            this.divergences = new();
            this.random = new Random();
        }

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // https://github.com/dotnet/orleans/pull/1772
            // https://github.com/dotnet/orleans/issues/8262
            // GetGrain<IManagementGrain>(0).GetHosts();
            // 
            var stats = await mgmt.GetDetailedGrainStatistics(); // new[] { "ProductActor" });
            this.nProductPartitions = stats.Where(w => w.GrainType.Contains("ProductActor")).Count();
            this.nOrderPartitions = stats.Where(w => w.GrainType.Contains("OrderActor")).Count();
            this.nShipmentPartitions = stats.Where(w => w.GrainType.Contains("ShipmentActor")).Count();
            this.nCustomerPartitions = stats.Where(w => w.GrainType.Contains("CustomerActor")).Count();
            // get customer partition
            int custPartition = (int)(this.customerId % nCustomerPartitions);
            var custActor = GrainFactory.GetGrain<ICustomerActor>(custPartition);
            this.customer = await custActor.GetCustomer(this.customerId);
        }

        public Task AddProduct(BasketItem item)
        {
            if (items.ContainsKey(item.ProductId))
            {
                _logger.LogInformation("Item already added to cart. Item will be updated.");
                items[item.ProductId] = item;
                return Task.CompletedTask;
            }
            items.Add(item.ProductId, item);
            return Task.CompletedTask;
        }

        public Task<Dictionary<long, BasketItem>> GetCart()
        {
            return Task.FromResult(items);
        }

        /*
         * TODO to discuss...
         * Indicates the customer signaling the checkout
         * Quotes the shipment of the products
         * Retrieves user payment info
         * Inthe future, get similar products to show to customer
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
        public async Task<Invoice> Checkout(CustomerCheckout basketCheckout)
        {
            if(items.Count == 0)
                throw new Exception("Cart is empty.");

            if(this.status == Status.CHECKOUT_SENT)
                throw new Exception("Cannot checkout a cart that has a checkout in progress");

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
            Checkout checkout = new(DateTime.Now, basketCheckout, this.items);
            // int orderPart = (int)(this.customerId % nOrderPartitions);
            // pick a random partition. why? (i) we do not know the order id yet (ii) distribute the work more seamlessly
            int orderPart = random.Next(0, nOrderPartitions);
            var orderActor = GrainFactory.GetGrain<IOrderActor>(orderPart);
            this.status = Status.CHECKOUT_SENT;
            // pass the responsibility
            Invoice resp = await orderActor.Checkout_1(checkout);

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
            return resp;
        }

        /**
         * Only used in a pure event-based model.
         * In Snapper, everything is synchronous, so cannot be used to eventually close a cart...
         */
        public Task Seal()
        {
            if (this.status == Status.CHECKOUT_SENT)
            {
                this.status = Status.OPEN;
                this.items.Clear();
                // history.Add()
                return Task.CompletedTask;
            }
            else
            {
                throw new Exception("Cannot seal a cart that has not been checked out");
            }
        }
    }
}

