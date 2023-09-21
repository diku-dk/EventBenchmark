namespace Common.Workload.CustomerWorker
{
    /**
     * The necessary data required by a customer worker to work properly
     */
    public class CustomerWorkerConfig
    {
        public int maxNumberKeysToBrowse { get; set; }

        public int maxNumberKeysToAddToCart { get; set; }

        // probability of a customer to checkout the cart
        public int checkoutProbability { get; set; }

        // products, carts, and customers
        public string productUrl { get; set; }

        public string cartUrl { get; set; }

        public Interval minMaxQtyRange { get; set; }

        public Interval delayBetweenRequestsRange { get; set; }

        public int voucherProbability { get; set; }

        // flag that defines the behavior of the customer worker
        // whether it will checkout directly or browse several items before
        public bool interactive { get; set; }

        public CustomerWorkerConfig(){}

        public CustomerWorkerConfig(int maxNumberKeysToBrowse, int maxNumberKeysToAddToCart, int checkoutProbability, string productUrl, string cartUrl,
            Interval minMaxQtyRange, Interval delayBetweenRequestsRange, int voucherProbability, bool interactive)
        {
            this.maxNumberKeysToBrowse = maxNumberKeysToBrowse;
            this.maxNumberKeysToAddToCart = maxNumberKeysToAddToCart;
            this.checkoutProbability = checkoutProbability;
            this.productUrl = productUrl;
            this.cartUrl = cartUrl;
            this.minMaxQtyRange = minMaxQtyRange;
            this.delayBetweenRequestsRange = delayBetweenRequestsRange;
            this.voucherProbability = voucherProbability;
            this.interactive = interactive;
        }

    }
}