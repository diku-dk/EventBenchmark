using System;
using Common.Scenario.Entity;
using Orleans;
using System.Threading.Tasks;
using Common.Entity;
using System.Collections.Generic;
using Orleans.Runtime;
using System.Linq;
using System.Text;
using Marketplace.Infra;
using Newtonsoft.Json;
using Marketplace.Message;

namespace Marketplace.Actor
{

    /**
     * Order actor does not coordinate with product actors.
     * Order only coordinate with stock actors.
     * This design favors higher useful work per time unit.
     * Since product is a user-facing microservice, most
     * customer requests target the product microservice.
     */
    public interface IOrderActor : IGrainWithIntegerKey, SnapperActor
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

        // https://dev.olist.com/docs/retrieving-order-informations
        private SortedList<long, List<OrderHistory>> orderHistory;

        public OrderActor()
		{
            this.nextOrderId = 1;
            this.orders = new();
            this.items = new();
            this.orderHistory = new();
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
          * The details about placing an order in olist:
          * https://dev.olist.com/docs/orders-notifications-details 
          * This transaction may not progress to payment
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
                // Should we store this order? what about the order items?
                // I dont think we should store orders that have not being created
                // If later a customer or seller cancels it, we can keep them in the
                // database. Here we can just return with a failure status.
                string res = JsonConvert.SerializeObject(checkout);
                // orderHistory.Add(DateTime.Now.Millisecond, res);
                return null;
                // TODO touching all other actors here
                // assuming most succeed, overhead is not too high
            }

            // TODO calculate total freight_value
            decimal total = 0;
            foreach (var item in checkout.items.Values)
            {
                total += (item.UnitPrice * item.Quantity);
            }

            decimal total_items = total;

            // apply vouchers, but only until total >= 0
            int v_idx = 0;
            decimal[] vouchers = checkout.customerCheckout.Vouchers;
            while (total > 0 && v_idx < vouchers.Length)
            {
                if(total - vouchers[v_idx] >= 0)
                {
                    total -= vouchers[v_idx];
                } else
                {
                    total = 0;
                }
            }

            // generate a global unique order ID
            // unique across partitions
            // string orderIdStr = this.orderActorId + "" + System.DateTime.Now.Millisecond + "" + nextOrderId;
            // long orderId = long.Parse(orderIdStr);
            Order newOrder = new()
            {
                id = nextOrderId,
                customer_id = checkout.customerCheckout.CustomerId,
                purchase_timestamp = checkout.createdAt.ToLongDateString(),
                // olist have seller acting in the approval process
                // here we approve automatically
                // besides, invoice is a request for payment, so it makes sense to use this status now
                status = OrderStatus.INVOICED.ToString(),
                approved_at = System.DateTime.Now.ToLongDateString(),
                total_amount = total,
                total_items = total_items
                // FIXME complete the other totals
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
                customer = checkout.customerCheckout,
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
            this.orders[orderId].status = status.ToString();
            return Task.CompletedTask;
        }

    }
}

