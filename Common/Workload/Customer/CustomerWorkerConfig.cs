using Common.Distribution;

namespace Common.Workload.Customer
{
    /**
     * The necessary data required by a customer worker to work properly
     */
    public sealed class CustomerWorkerConfig
	{
        public int maxNumberKeysToBrowse { get; set; }

        public int maxNumberKeysToAddToCart { get; set; }

        public DistributionType sellerDistribution { get; set; }

        public Interval sellerRange { get; set; }

        // probability of a customer to checkout the cart
        public int checkoutProbability { get; set; } = 50;

        // products, carts, and customers
        public string productUrl { get; set; }

        public string cartUrl { get; set; }

        public Interval minMaxQtyRange { get; set; }

        public Interval delayBetweenRequestsRange { get; set; }

        public int voucherProbability { get; set; } = 10;

        public int maxNumberVouchers { get; set; } = 10;

        // flag that defines the behavior of the customer worker
        // whether it will checkout directly or browse several items before
        public bool interactive { get; set; }
    }
}