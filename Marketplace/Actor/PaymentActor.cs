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

namespace Marketplace.Actor
{

    public interface IPaymentActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task ProcessPayment(Invoice invoice);
    }

    public class PaymentActor : Grain, IPaymentActor
	{
        private long paymentActorId;
        private long nStockPartitions;
        private long nCustomerPartitions;
        private readonly Random random;

        // DB
        // key order_id
        private Dictionary<long, List<OrderPayment>> payments;
        private Dictionary<long, OrderPaymentCard> cardPayments;

        public PaymentActor()
		{
            this.random = new Random();
		}

        public override async Task OnActivateAsync()
        {
            this.paymentActorId = this.GetPrimaryKeyLong();
            var mgmt = GrainFactory.GetGrain<IManagementGrain>(0);
            var stats = await mgmt.GetDetailedGrainStatistics();
            this.nStockPartitions = stats.Where(w => w.GrainType.Contains("StockActor")).Count();
            this.nCustomerPartitions = stats.Where(w => w.GrainType.Contains("CustomerActor")).Count();
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
                approved = false;

            return approved;
        }

        public async Task ProcessPayment(Invoice invoice)
        {

            bool approved = await ContactESP(invoice.customer, invoice.order.total_amount);
            List<Task> tasks = new();

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
            IOrderActor orderActor = GrainFactory.GetGrain<IOrderActor>(invoice.orderActorId);
            var custPartition = (invoice.order.customer_id % nCustomerPartitions);
            ICustomerActor custActor = GrainFactory.GetGrain<ICustomerActor>(custPartition);
            // shipment actor is the same actor id of order
            IShipmentActor shipmentActor = GrainFactory.GetGrain<IShipmentActor>(invoice.orderActorId);
            if (approved)
            {
                // ?? what is the status processing? should come before or after payment? before is INVOICED, so can only come after. but shipment sets to shipped...
                // I think processing is when the seller must approve or not the order, but here all orders are approved by default. so we dont use processing
                // notify
                tasks.Add(orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.PAYMENT_PROCESSED));
                tasks.Add(custActor.NotifyPayment(invoice.customer.CustomerId));
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
                // an event approach would avoid the redundancy of contacting several actors to notify about the same fact
                tasks.Add( orderActor.UpdateOrderStatus(invoice.order.id, OrderStatus.PAYMENT_FAILED) );
                // notify again because the shipment would have called it in case of successful payment
                tasks.Add(orderActor.noOp());
                tasks.Add( custActor.NotifyPayment(invoice.customer.CustomerId, false) );
                tasks.Add( shipmentActor.noOp() );
            }

            await Task.WhenAll(tasks);

        }

    }
}

