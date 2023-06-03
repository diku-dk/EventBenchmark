using System;
using Common.Entity;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Marketplace.Infra;
using Newtonsoft.Json;
using Marketplace.Message;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;
using Orleans.Concurrency;

namespace Marketplace.Actor
{
    [Reentrant]
    public class OrderActor : Grain, IOrderActor
    {
        // partitions
        private int nStockPartitions;
        private int nCustomerPartitions;
        private int nOrderPartitions;
        private int nPaymentPartitions;
        private int nShipmentPartitions;

        private long orderActorId;
        // it represents all orders in this partition
        private long nextOrderId;
        private long nextHistoryId;

        // database
        private readonly Dictionary<long, Order> orders;
        private readonly Dictionary<long, List<OrderItem>> items;

        // https://dev.olist.com/docs/retrieving-order-informations
        private readonly SortedList<long, List<OrderHistory>> history;

        private readonly ILogger<OrderActor> _logger;

        private static readonly decimal[] emptyArray = Array.Empty<decimal>();

        public OrderActor(ILogger<OrderActor> _logger)
        {
            this.nextOrderId = 1;
            this.nextHistoryId = 1;
            this.orders = new();
            this.items = new();
            this.history = new();
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.orderActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "StockActor", "CustomerActor", "OrderActor", "PaymentActor", "ShipmentActor" });
            this.nStockPartitions = dict["StockActor"];
            this.nCustomerPartitions = dict["CustomerActor"];
            this.nOrderPartitions = dict["OrderActor"];
            this.nPaymentPartitions = dict["PaymentActor"];
            this.nShipmentPartitions = dict["ShipmentActor"];

            this._logger.LogWarning("Order grain {0} activated: #stock grains {1} #order grains {2} #payment grains {3} ", this.orderActorId, nStockPartitions, nOrderPartitions, nPaymentPartitions);
        }

        private long GetNextOrderId()
        {
            while(this.nextOrderId % this.nOrderPartitions != this.orderActorId)
            {
                this.nextOrderId++;
            }
            return this.nextOrderId;
        }

        /**
          * The details about placing an order in olist:
          * https://dev.olist.com/docs/orders-notifications-details 
          * This transaction may not progress to payment
         */
        public async Task Checkout(Checkout checkout)
        {
            this._logger.LogWarning("Order grain {0} -- Checkout process starting for customer {0}", this.orderActorId, checkout.customerCheckout.CustomerId);
            List<Task<ItemStatus>> statusResp = new(checkout.items.Count);

            foreach (var item in checkout.items)
            {
                long partition = (item.ProductId % nStockPartitions);
                var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                statusResp.Add(stockActor.AttemptReservation(item.ProductId, item.Quantity));
            }

            await Task.WhenAll(statusResp);

            bool abort = statusResp.Where(c => c.Result != ItemStatus.IN_STOCK).Count() > 0;

            List<Task> tasks = new(checkout.items.Count);

            // only cancel the ones who have had stock decreased!
            List<long> unavailableItems = new();
            if (abort)
            {
                int idx = 0;
                this._logger.LogWarning("Order part {0} -- Checkout process aborted for customer {0}", this.orderActorId, checkout.customerCheckout.CustomerId);
                foreach (var item in checkout.items)
                {
                    long partition = (item.ProductId % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    if (statusResp[idx].Result != ItemStatus.IN_STOCK)
                    {
                        unavailableItems.Add(item.ProductId);
                        tasks.Add(stockActor.CancelReservation(item.ProductId, item.Quantity));
                    } else
                    {
                        tasks.Add(stockActor.noOp());
                    }
                    idx++;
                }
            }
            else
            {
                foreach (var item in checkout.items)
                {
                    long partition = (item.ProductId % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    tasks.Add(stockActor.ConfirmReservation(item.ProductId, item.Quantity));
                }
            }

            await Task.WhenAll(tasks);

            // generate a global unique order ID
            // unique across partitions
            long orderId = GetNextOrderId();

            if (abort)
            {
                // Should we store this order? what about the order items?
                // I dont think we should store orders that have not being created

                Order failedOrder = new()
                {
                    id = orderId,
                    customer_id = checkout.customerCheckout.CustomerId,
                    status = OrderStatus.CANCELED,
                    purchase_date = checkout.createdAt,
                    created_at = System.DateTime.Now,
                    data = JsonConvert.SerializeObject(checkout),
                };
                this.orders.Add(orderId, failedOrder);

                await ProcessFailedOrder(checkout.customerCheckout.CustomerId, orderId, unavailableItems);
                this._logger.LogWarning("Order part {0} -- Checkout process failed for customer {1} -- Order id is {2}",
                    this.orderActorId, checkout.customerCheckout.CustomerId, orderId);

                return;
                // assuming most succeed, overhead is not too high
            }

            // calculate total freight_value
            decimal total_freight = 0;
            foreach (var item in checkout.items)
            {
                total_freight += item.FreightValue;
            }

            decimal total_amount = 0;
            foreach (var item in checkout.items)
            {
                total_amount += (item.UnitPrice * item.Quantity);
            }

            decimal total_items = total_amount;

            // apply vouchers, but only until total >= 0
            int v_idx = 0;
            decimal[] vouchers = checkout.customerCheckout.Vouchers == null ? emptyArray : checkout.customerCheckout.Vouchers;
            decimal total_incentive = 0;
            while (total_amount > 0 && v_idx < vouchers.Length)
            {
                if (total_amount - vouchers[v_idx] >= 0)
                {
                    total_amount -= vouchers[v_idx];
                    total_incentive += vouchers[v_idx];
                } else
                {
                    total_amount = 0;
                }
            }

            Order newOrder = new()
            {
                id = orderId,
                customer_id = checkout.customerCheckout.CustomerId,
                // olist have seller acting in the approval process
                // here we approve automatically
                // besides, invoice is a request for payment, so it makes sense to use this status now
                status = OrderStatus.INVOICED,
                created_at = System.DateTime.Now,
                purchase_date = checkout.createdAt,
                total_amount = total_amount,
                total_items = total_items,
                total_freight = total_freight,
                total_incentive = total_incentive,
                total_invoice = total_amount + total_freight,
                count_items = checkout.items.Count(),

            };
            this.orders.Add(orderId, newOrder);

            List<OrderItem> orderItems = new(checkout.items.Count);
            int id = 0;
            foreach (var item in checkout.items)
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

            this.items.Add(orderId, orderItems);

            // initialize order history
            this.history.Add(orderId, new List<OrderHistory>() { new OrderHistory()
            {
                id = this.nextHistoryId,
                created_at = newOrder.created_at, // redundant, but it is what it is...
                status = OrderStatus.INVOICED,

            } });

            Invoice invoice = new Invoice( checkout.customerCheckout, newOrder, orderItems);

            // increment
            this.nextOrderId++;
            this.nextHistoryId++;

            this._logger.LogWarning("Order part {0} -- Checkout process succeeded for customer {1} -- Order id is {2}",
                this.orderActorId, checkout.customerCheckout.CustomerId, orderId);
            
            long paymentActor = newOrder.id % nPaymentPartitions;
            await this.GrainFactory.GetGrain<IPaymentActor>(paymentActor).ProcessPayment(invoice);
 
            return;

        }

        /**
         * Olist prescribes that order status is "delivered" if at least one order item has been delivered
         * Based on https://dev.olist.com/docs/orders
         */
        public Task UpdateOrderStatus(long orderId, OrderStatus status)
        {
            this._logger.LogWarning("Order part {0} -- Updating order status for order id {1}", this.orderActorId, orderId);

            if (!this.orders.ContainsKey(orderId))
            {
                string str = new StringBuilder().Append("Order ").Append(orderId)
                    .Append(" cannot be found to update to status ").Append(status.ToString()).ToString();
                throw new Exception(str);
            }

            var now = DateTime.Now;

            OrderHistory orderHistory = null;

            // on every update, update the field updated_at in the order
            this.orders[orderId].updated_at = now;
            var oldStatus = this.orders[orderId].status;
            this.orders[orderId].status = status;

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
            // dont need the second check since the shipment is supposed to keep track
            if(status == OrderStatus.DELIVERED)
            {
                this.orders[orderId].delivered_customer_date = now;
            }

            orderHistory = new()
            {
                id = this.nextHistoryId,
                created_at = now,
                status = status
            };

            this.history[orderId].Add(orderHistory);

            this.nextHistoryId++;

            this._logger.LogWarning("Order part {0} -- Updated order status of order id {1} from {2} to {3}", this.orderActorId, orderId, oldStatus, this.orders[orderId].status);

            return Task.CompletedTask;
        }

        public Task<List<Order>> GetOrders(long customerId, Predicate<Order> predicate = null)
        {
            List<Order> res;
            if (predicate is not null)
                res = this.orders.Select(o => o.Value).Where(p => p.customer_id == customerId && predicate.Invoke(p)).ToList();
            else
                res = this.orders.Select(o => o.Value).Where(p => p.customer_id == customerId).ToList();
            return Task.FromResult(res);
        }

        public async Task ProcessFailedOrder(long customerId, long orderId, List<long> unavailableItems)
        {
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(orderId % nOrderPartitions);
            ICustomerActor custActor = GrainFactory.GetGrain<ICustomerActor>(customerId % nCustomerPartitions);
            IShipmentActor shipmentActor = GrainFactory.GetGrain<IShipmentActor>(orderId % nShipmentPartitions);
            IPaymentActor payActor = GrainFactory.GetGrain<IPaymentActor>(orderId % nPaymentPartitions);
            List<Task> tasks = new(4);

            tasks.Add(orderActor.UpdateOrderStatus(orderId, OrderStatus.PAYMENT_FAILED));
            tasks.Add(orderActor.noOp());
            tasks.Add(payActor.noOp());
            tasks.Add(custActor.NotifyFailedPayment(customerId, null));
            tasks.Add(shipmentActor.noOp());

            await Task.WhenAll(tasks);
        }


    }
}

