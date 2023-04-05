using System;
using System.Collections.Generic;
using Common.Scenario.Entity;

namespace Marketplace.Entity
{
    /*
     * "An invoice acts as a request for payment for the delivery of goods or services."
	 * Source: https://invoice.2go.com/learn/invoices/invoice-vs-purchase-order/
	 * An invoice data structure contains all necessary info for the payment actor to process
	 * a payment
	 */
    public class Invoice
	{

		public long orderActorId;

		public CustomerCheckout customer;

		public Order order;

		// items
		public List<OrderItem> items;

		// inconsistencies, if any
		// public List<ProductCheck> inconsistencies;
	}
}

