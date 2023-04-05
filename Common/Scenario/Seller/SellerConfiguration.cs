using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Scenario.Seller
{

	public sealed class SellerConfiguration
	{
        // to serve customer workers
        // the seller should know the dist of its own products
        public Distribution keyDistribution;

        // 0 = keys 1 = category
        public int[] typeUpdateDistribution = new int[] { 0, 1 };

        // the perc of increase
        public Range adjustRange = new Range(1, 21);

        // seller main page (the one after login), seller products page
        public Dictionary<string, string> urls;

        public int delayBeforeStart = 0;

        public Range delayBetweenRequestsRange;

    }
}

