using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Scenario.Seller
{

	public sealed class SellerConfiguration
	{
        //public int numMaxProdToUpdate = 3;

        // public Distribution keyDistribution;

        // 0 = keys 1 = category
        public int[] typeUpdateDistribution = new int[] { 0, 1 };

        // the perc of increase
        public Range adjustRange = new Range(1, 21);

        // product
        public string productsUrl;

        public int delayBeforeStart = 0;

    }
}

