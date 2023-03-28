using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Scenario.Customer
{
	public sealed class CustomerConfiguration
	{
        public int maxNumberKeysToBrowse;

        public int maxNumberKeysToAddToCart;

        public Distribution sellerDistribution;

        public Range sellerRange;

        // expectation: same across all sellers
        // without this each seller can have a custom distribution...
        // public Distribution productDistribution;

        // probability of a customer to checkout the cart
        public int[] checkoutDistribution = new int[] { 1 };

        // products, carts
        public Dictionary<string, string> urls;

        public Range minMaxQtyRange;

        public int delayBeforeStart = 0;

        public Range delayBetweenRequestsRange;

    }
}