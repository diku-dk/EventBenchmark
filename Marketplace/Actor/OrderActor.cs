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
using Marketplace.Interfaces;

namespace Marketplace.Actor
{

    public class OrderActor : Grain, IOrderActor
    {
        private long nStockPartitions;
        private long nOrderPartitions;
        private long orderActorId;
        // it represents all orders in this partition
        private long nextOrderId;
        private long nextHistoryId;

        // database
        private Dictionary<long, Order> orders;
        private Dictionary<long, List<OrderItem>> items;

        // https://dev.olist.com/docs/retrieving-order-informations
        private SortedList<long, List<OrderHistory>> history;

        public OrderActor()
        {
            this.nextOrderId = 1;
            this.nextHistoryId = 1;
            this.orders = new();
            this.items = new();
            this.history = new();
        }

        public override async Task OnActivateAsync()
        {
            this.orderActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            var stats = await mgmt.GetDetailedGrainStatistics();
            this.nStockPartitions = stats.Where(w => w.GrainType.Contains("StockActor")).Count();
            this.nOrderPartitions = stats.Where(w => w.GrainType.Contains("Orderctor")).Count();
        }

        private long GetNextOrderId()
        {
            while(nextOrderId % nOrderPartitions != orderActorId)
            {
                nextOrderId++;
            }
            return nextOrderId;
        }

        /**
          * The details about placing an order in olist:
          * https://dev.olist.com/docs/orders-notifications-details 
          * This transaction may not progress to payment
         */
        public async Task<Invoice> Checkout_1(Checkout checkout)
        {
            List<Task<ItemStatus>> statusResp = new(checkout.items.Count);

            foreach (var item in checkout.items)
            {
                long partition = (item.Key % nStockPartitions);
                var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                statusResp.Add(stockActor.AttemptReservation(item.Key, item.Value.Quantity));
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
                if (total - vouchers[v_idx] >= 0)
                {
                    total -= vouchers[v_idx];
                } else
                {
                    total = 0;
                }
            }

            long orderId = GetNextOrderId();

            // generate a global unique order ID
            // unique across partitions
            // string orderIdStr = this.orderActorId + "" + System.DateTime.Now.Millisecond + "" + nextOrderId;
            // long orderId = long.Parse(orderIdStr);
            Order newOrder = new()
            {
                id = orderId,
                customer_id = checkout.customerCheckout.CustomerId,
                purchase_timestamp = checkout.createdAt.ToLongDateString(),
                // olist have seller acting in the approval process
                // here we approve automatically
                // besides, invoice is a request for payment, so it makes sense to use this status now
                status = OrderStatus.INVOICED.ToString(),
                created_at = System.DateTime.Now.ToLongDateString(),
                total_amount = total,
                total_items = total_items
                // TODO complete the other totals
            };
            orders.Add(orderId, newOrder);

            List<OrderItem> orderItems = new(checkout.items.Count);
            int id = 0;
            foreach (var item in checkout.items.Values)
            {
                orderItems.Add(
                    new()
                    {
                        order_id = orderId,
                        order_item_id = id,
                        product_id = item.ProductId,
                        seller_id = item.SellerId,
                        unit_price = item.UnitPrice,
                        quantity = item.Quantity,
                        total_items = item.UnitPrice * item.Quantity,
                        total_amount = (item.Quantity * item.FreightValue) + (item.Quantity * item.UnitPrice) // freight value applied per item by default
                    }
                    );
                id++;
            }

            items.Add(orderId, orderItems);

            // initialize order history
            history.Add(orderId, new List<OrderHistory>() { new OrderHistory()
            {
                id = nextHistoryId,
                created_at = newOrder.created_at, // redundant, but it is what it is...
                status = OrderStatus.INVOICED.ToString(),

            } });

            Invoice resp = new()
            {
                customer = checkout.customerCheckout,
                order = newOrder,
                items = orderItems
            };

            // increment
            nextOrderId++;
            nextHistoryId++;
            return resp;

        }

        /**
         * Olist prescribes that order status is "delivered" if at least one order item has been delivered
         * Based on https://dev.olist.com/docs/orders
         */
        public Task UpdateOrderStatus(long orderId, OrderStatus status)
        {
            if (!this.orders.ContainsKey(orderId))
            {
                string str = new StringBuilder().Append("Order ").Append(orderId)
                    .Append(" cannot be found to update to status ").Append(status.ToString()).ToString();
                throw new Exception(str);
            }

            string now = DateTime.Now.ToLongDateString();

            OrderHistory orderHistory = null;

            // on every update, update the field updated_at in the order
            this.orders[orderId].updated_at = now;
            this.orders[orderId].status = status.ToString();

            // on shipped status, update delivered_carrier_date and estimated_delivery_date. add the entry
            if (status == OrderStatus.SHIPPED)
            {
                this.orders[orderId].delivered_carrier_date = now;
                this.orders[orderId].estimated_delivery_date = now;
            }

            // on payment failure or success, update payment_date and add the respective entry
            if (status == OrderStatus.PAYMENT_PROCESSED || status == OrderStatus.PAYMENT_FAILED)
            {
                this.orders[orderId].payment_date = now; 
            }

            // on first delivery, update delivered customer date
            // dont need the second check once the shipment is already keeping track
            if(status == OrderStatus.DELIVERED) //&& !this.orders[orderId].Equals(OrderStatus.DELIVERED.ToString()))
            {
                this.orders[orderId].delivered_customer_date = now;
            }

            orderHistory = new()
            {
                created_at = now,
                status = status.ToString()
            };

            history[orderId].Add(orderHistory);

            return Task.CompletedTask;
        }

    }
}

