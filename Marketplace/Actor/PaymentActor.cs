using System;
using Common.Entity;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;
using System.Linq;
using System.Collections.Generic;
using Common.Scenario.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;
using System.Collections.ObjectModel;

namespace Marketplace.Actor
{

    public class PaymentActor : Grain, IPaymentActor
	{
        private long paymentActorId;
        private long nStockPartitions;
        private long nCustomerPartitions;
        private long nOrderPartitions;
        private long nShipmentPartitions;
        private readonly Random random;

        // DB
        // key order_id
        private readonly Dictionary<long, List<OrderPayment>> payments;
        private readonly Dictionary<long, OrderPaymentCard> cardPayments;

        private readonly ILogger<PaymentActor> _logger;

        public PaymentActor(ILogger<PaymentActor> _logger)
		{
            this.random = new Random();
            this.payments = new();
            this.cardPayments = new();
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.paymentActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "StockActor", "OrderActor", "ShipmentActor", "CustomerActor" });
            this.nStockPartitions = dict["StockActor"];
            this.nCustomerPartitions = dict["CustomerActor"];
            this.nOrderPartitions = dict["OrderActor"];
            this.nShipmentPartitions = dict["ShipmentActor"];
            this._logger.LogWarning("Payment grain {0} activated: #stock grains {1} #customer grains {2} #order grains {3} ", this.paymentActorId, nStockPartitions, nCustomerPartitions, nOrderPartitions);
        }

        /**
         * simulate an external request by adding a random delay
         * customer checkout is necessary here to contact external service provider
         */
        public async Task<bool> ContactESP(CustomerCheckout customer, decimal value)
        {
            bool approved = true;
            await Task.Delay(this.random.Next(100, 1001));

            // TODO pick from a distribution
            if (this.random.Next(1, 11) > 7)
            {
                // approved = false;
                _logger.LogWarning("Payment grain {0}, order would have failed!", this.paymentActorId);
            }

            return approved;
        }

        public async Task ProcessPayment(Invoice invoice)
        {
            this._logger.LogWarning("Payment grain {0} -- Payment process starting for order {0}", this.paymentActorId, invoice.order.id);
            bool approved = await ContactESP(invoice.customer, invoice.order.total_amount);
            List<Task> tasks = new(invoice.items.Count);

            if (approved)
            {
                foreach (var item in invoice.items)
                {
                    long partition = (item.product_id % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    tasks.Add(stockActor.ConfirmOrder(item.product_id, item.quantity));
                }
            } else
            {
                foreach (var item in invoice.items)
                {
                    long partition = (item.product_id % nStockPartitions);
                    var stockActor = GrainFactory.GetGrain<IStockActor>(partition);
                    tasks.Add(stockActor.CancelReservation(item.product_id, item.quantity));
                }
            }

            await Task.WhenAll(tasks);

            tasks.Clear();

            // call order, customer, and shipment
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(invoice.order.id % nOrderPartitions);
            var custPartition = (invoice.order.customer_id % nCustomerPartitions);
            ICustomerActor custActor = GrainFactory.GetGrain<ICustomerActor>(custPartition);
            // shipment actor is the same actor id of order
            IShipmentActor shipmentActor = GrainFactory.GetGrain<IShipmentActor>(invoice.order.id % nShipmentPartitions);
            if (approved)
            {
                this._logger.LogWarning("Payment grain {0} -- Payment process suceeded for order {0}", this.paymentActorId, invoice.order.id);

                // ?? what is the status processing? should come before or after payment?
                // before is INVOICED, so can only come after. but shipment sets to shipped...
                // I think processing is when the seller must approve or not the order,
                // but here all orders are approved by default. so we dont use processing
                // notify
                tasks.Add(orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.PAYMENT_PROCESSED));
                tasks.Add(custActor.NotifyPayment(invoice.customer.CustomerId, invoice.order));
                tasks.Add(shipmentActor.ProcessShipment(invoice));

                List<OrderPayment> paymentLines = new();
                int seq = 1;

                // create payment tuples
                if (invoice.customer.PaymentType.Equals( PaymentType.CREDIT_CARD.ToString() ) || invoice.customer.PaymentType.Equals(PaymentType.DEBIT_CARD.ToString()))
                { 
                    paymentLines.Add(new OrderPayment()
                    {
                        order_id = invoice.order.id,
                        payment_sequential = seq,
                        payment_type = invoice.customer.PaymentType,
                        payment_installments = invoice.customer.Installments,
                        payment_value = invoice.order.total_amount
                    });

                    // create an entity for credit card payment details with FK to order payment
                    OrderPaymentCard card = new()
                    {
                        order_id = invoice.order.id,
                        payment_sequential = seq,
                        card_number = invoice.customer.CardNumber,
                        card_holder_name = invoice.customer.CardHolderName,
                        card_expiration = invoice.customer.CardExpiration,
                        // I guess firms don't save this data in this table to avoid leaks...
                        // card_security_number = invoice.customer.CardSecurityNumber,
                        card_brand = invoice.customer.CardBrand
                    };

                    cardPayments.Add(invoice.order.id, card);
                    seq++;
                }

                if (invoice.customer.PaymentType.Equals(PaymentType.BOLETO.ToString())){
                    paymentLines.Add(new OrderPayment()
                    {
                        order_id = invoice.order.id,
                        payment_sequential = seq,
                        payment_type = invoice.customer.PaymentType,
                        payment_installments = 1,
                        payment_value = invoice.order.total_amount
                    });
                    seq++;
                }

                // then one line for each voucher
                if (invoice.customer.Vouchers != null)
                {
                    foreach (var voucher in invoice.customer.Vouchers)
                    {
                        paymentLines.Add(new OrderPayment()
                        {
                            order_id = invoice.order.id,
                            payment_sequential = seq,
                            payment_type = PaymentType.VOUCHER.ToString(),
                            payment_installments = 1,
                            payment_value = voucher
                        });
                        seq++;
                    }
                }

                payments.Add(invoice.order.id, paymentLines);

            }
            else
            {

                this._logger.LogWarning("Payment grain {0} -- Payment process failed for order {0}", this.paymentActorId, invoice.order.id);

                // an event approach would avoid the redundancy of contacting several actors to notify about the same fact
                tasks.Add( orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.PAYMENT_FAILED) );
                // notify again because the shipment would have called it in case of successful payment
                tasks.Add( orderActor.noOp() );
                tasks.Add( custActor.NotifyFailedPayment(invoice.customer.CustomerId, null) );
                tasks.Add( shipmentActor.noOp() );
            }

            await Task.WhenAll(tasks);

        }

    }
}

