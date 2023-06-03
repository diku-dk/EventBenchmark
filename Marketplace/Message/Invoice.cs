using System.Collections.Generic;
using Common.Event;
using Common.Entity;

namespace Marketplace.Message
{
    /*
     * "An invoice acts as a request for payment for the delivery of goods or services."
	 * Source: https://invoice.2go.com/learn/invoices/invoice-vs-purchase-order/
	 * An invoice data structure contains all necessary info for the payment 
	 * actor to process a payment
	 */
    public record Invoice(CustomerCheckout customer, Order order, IList<OrderItem> items);

}

