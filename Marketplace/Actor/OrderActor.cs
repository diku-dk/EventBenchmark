using System;
using Common.Scenario.Entity;
using Orleans;
using System.Threading.Tasks;
using Marketplace.Entity;
using System.Collections.Generic;
using Orleans.Runtime;
using System.Linq;
using System.Text;

namespace Marketplace.Actor
{

    /**
     * Order actor does not coordinate with product actors.
     * Order only coordinate with stock actors.
     * This design favors higher useful work per time unit.
     * Since product is a user-facing microservice, most
     * customer requests target the product microservice.
     */
    public interface IOrderActor : IGrainWithIntegerKey
    {
        public Task<Invoice> Checkout_1(Checkout checkout);
        public Task UpdateOrderStatus(long orderId, OrderStatus status);
    }

    public class OrderActor : Grain, IOrderActor
	{
        private long nStockPartitions;
        private long orderActorId;
        // it represents all orders in this partition
        private long nextOrderId;

        // database
        private Dictionary<long, Order> orders;
        private Dictionary<long, List<OrderItem>> items;

        public OrderActor()
		{
            this.orders = new();
            this.items = new();
        }

        public override async Task OnActivateAsync()
        {
            this.orderActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            // https://github.com/dotnet/orleans/pull/1772
            // https://github.com/dotnet/orleans/issues/8262
            // GetGrain<IManagementGrain>(0).GetHosts();
            // 
            var stats = await mgmt.GetDetailedGrainStatistics(); // new[] { "ProductActor" });
            this.nStockPartitions = stats.Where(w => w.GrainType.Contains("StockActor")).Count();
        }

        /**
          * TODO discuss
          * This transaction may not progress to payment
          * due to problems in stock or unavailability of products.
          * Perhaps needs some adjustment in Snapper? or cut this checkout in two separated transactions?
          * -- other options: (i) remove product delete transaction (ii) abort checkouts with delete products (no feedback from customer)
          */
        public async Task<Invoice> Checkout_1(Checkout checkout)
        {
            List<Task<ItemStatus>> statusResp = new(checkout.items.Count);

            foreach(var item in checkout.items)
            {
                long partition = (item.Key % nStockPartitions);
                var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                statusResp.Add( stockActor.AttemptReservation(item.Key, item.Value.Quantity) );
            }

            await Task.WhenAll(statusResp);

            bool abort = false;
            int idx = 0;
            foreach (var item in checkout.items)
            {
                if (statusResp[idx].Result != ItemStatus.IN_STOCK)
                {
                    abort = true;
                    break;
                }
            }

            List<Task> tasks = new(checkout.items.Count);

            if (abort)
            {
                foreach (var item in checkout.items)
                {
                    long partition = (item.Key % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    tasks.Add(stockActor.CancelReservation(item.Key, item.Value.Quantity));
                }
            }
            else
            {
                foreach (var item in checkout.items)
                {
                    long partition = (item.Key % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    tasks.Add(stockActor.ConfirmReservation(item.Key, item.Value.Quantity));
                }
            }

            await Task.WhenAll(tasks);

            if (abort)
            {
                return null;
            }

            // calculate total
            // TODO apply vouchers
            decimal total = 0;
            foreach (var item in checkout.items.Values)
            {
                total += item.UnitPrice * item.Quantity;
            }

            // generate a global unique order ID
            // unique across partitions
            // string orderIdStr = this.orderActorId + "" + System.DateTime.Now.Millisecond + "" + nextOrderId;
            // long orderId = long.Parse(orderIdStr);
            Order newOrder = new()
            {
                order_id = nextOrderId,
                customer_id = checkout.customer.CustomerId,
                order_status = OrderStatus.INVOICED.ToString(),
                order_purchase_timestamp = checkout.createdAt.ToLongDateString(),
                // olist seems to have seller acting in the approval process
                // here we approve automatically
                order_approved_at = System.DateTime.Now.ToLongDateString(),
                total = total
            };
            orders.Add(nextOrderId, newOrder);

            List<OrderItem> orderItems = new(checkout.items.Count);
            int id = 0;
            foreach(var item in checkout.items.Values)
            {
                orderItems.Add(
                    new()
                    {
                        order_id = nextOrderId,
                        order_item_id = id,
                        product_id = item.ProductId,
                        seller_id = item.SellerId,
                        price = item.UnitPrice,
                        quantity = item.Quantity
                    }
                    );
                id++;
            }

            Invoice resp = new()
            {
                orderActorId = this.orderActorId,
                customer = checkout.customer,
                order = newOrder,
                items = orderItems
            };

            // increment
            nextOrderId++;
            return resp;

        }

        public Task UpdateOrderStatus(long orderId, OrderStatus status)
        {
            if (!this.orders.ContainsKey(orderId))
            {
                string str = new StringBuilder().Append("Order ").Append(orderId)
                    .Append(" cannot be found to update to status ").Append(status.ToString()).ToString();
                throw new Exception(str);
            }
            this.orders[orderId].order_status = status.ToString();
            return Task.CompletedTask;
        }

        private static readonly Invoice defaultResponse = new();

    }
}

