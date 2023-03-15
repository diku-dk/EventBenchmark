using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Scenario.Customer
{

	public sealed class CustomerConfiguration
	{
        public int maxNumberKeysToBrowse;
        
        public Distribution keyDistribution;

        // probability of a customer to checkout the cart
        public int[] checkoutDistribution = new int[] { 1 };

        public Range keyRange;

        // product, cart
        public Dictionary<string, string> urls;

        public Range minMaxQtyRange;

        public int maxNumberKeysToAddToCart;

        public Range delayBetweenRequestsRange;

        public int delayBeforeStart;

        // public Func<string, int, string> buildCartItemPayloadFunc;

        public Guid streamId;
    }
}

