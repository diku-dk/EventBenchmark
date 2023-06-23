using System.Collections.Generic;
using Common.Distribution;

namespace Common.Workload.Seller
{
    /**
     * The necessary data required by a seller worker to work properly
     */
	public sealed class SellerWorkerConfig
	{
        // to serve customer workers
        // the seller should know the dist of its own products
        public DistributionType keyDistribution;

        // 0 = keys 1 = category
        public int[] typeUpdateDistribution = new int[] { 0, 1 };

        // the perc of increase
        public Interval adjustRange = new Interval(1, 20);

        // seller main page (the one after login), seller products page
        public Dictionary<string, string> urls;

        public Interval delayBetweenRequestsRange;

    }
}

