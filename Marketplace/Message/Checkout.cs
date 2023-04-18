using System;
using Common.Scenario.Entity;
using System.Collections.Generic;

namespace Marketplace.Message
{
	/**
	 * Assemble all data necessary for processing a checkout
	 * Data structure delivered to order actor to proceed with order processing
	 */
	public struct Checkout
	{
		public readonly DateTime createdAt;
		public readonly CustomerCheckout customerCheckout;
		public readonly IDictionary<long, BasketItem> items;

        public Checkout(DateTime createdAt, CustomerCheckout customerCheckout, IDictionary<long, BasketItem> items)
		{
			this.createdAt = createdAt;
			this.customerCheckout = customerCheckout;
			this.items = items;
		}

    }
}

