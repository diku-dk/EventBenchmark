using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Customer
{
	public sealed class CustomerConfiguration
	{
        public readonly int maxNumberKeysToBrowse;
        
        public readonly Distribution keyDistribution;

        // probability of a customer to checkout the cart
        // public readonly Distribution checkoutDistribution;

        public readonly Range keyRange;

        // product, cart
        public readonly Dictionary<string, string> urls;

        public readonly Range minMaxQtyRange;

        public readonly int maxNumberKeysToAddToCart;

        public readonly Range delayBetweenRequestsRange;

        public readonly int delayBeforeStart;

        public readonly Func<long, int, string> BuildCartItemPayloadFunc;

    }
}

