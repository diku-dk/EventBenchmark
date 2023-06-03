using System;
using Common.Entity;
using Common.Event;
using System.Collections.Generic;

namespace Marketplace.Message
{
	/**
	 * Assemble all data necessary for processing a checkout
	 * Data structure delivered to order actor to proceed with order processing
	 */
	public record Checkout (DateTime createdAt,
							CustomerCheckout customerCheckout,
							List<CartItem> items);
}

