using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Scenario.Customer
{
    /**
     * The necessary data required by a customer worker to work properly
     */
    public sealed class CustomerConfiguration
	{
        public int maxNumberKeysToBrowse;

        public int maxNumberKeysToAddToCart;

        public Distribution sellerDistribution;

        public Interval sellerRange;

        // probability of a customer to checkout the cart
        public int checkoutProbability = 50;

        // products, carts
        public Dictionary<string, string> urls;

        public Interval minMaxQtyRange;

        public int delayBeforeStart = 0;

        public Interval delayBetweenRequestsRange;

    }
}