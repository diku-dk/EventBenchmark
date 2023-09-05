namespace Common.Workload.Seller
{
    /**
     * The necessary data required by a seller worker to work properly
     */
    public class SellerWorkerConfig
    {
        // the perc of increase
        public Interval adjustRange { get; set; }

        public string productUrl { get; set; }

        public string sellerUrl { get; set; }

        public Interval delayBetweenRequestsRange { get; set; }

        public bool interactive { get; set; }

    }
}

