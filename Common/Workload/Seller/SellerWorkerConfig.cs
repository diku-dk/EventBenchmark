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
        public DistributionType keyDistribution { get; set; }

        // 0 = keys 1 = category
        public int[] typeUpdateDistribution { get; set; } = new int[] { 0, 1 };

        // the perc of increase
        public Interval adjustRange { get; set; } = new Interval(1, 20);

        public string productUrl { get; set; }
        public string sellerUrl { get; set; }

        public Interval delayBetweenRequestsRange { get; set; }
    }
}

