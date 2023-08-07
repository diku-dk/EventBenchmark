using Common.Workload.CustomerWorker;
using Common.Workload.Seller;
using Common.Streaming;
using Common.Distribution;
using Common.Workload.Delivery;

namespace Common.Workload
{

    public class WorkloadConfig
    {
        public string connectionString { get; set; } = "Data Source=file.db"; // "DataSource=:memory:"

        // maximum number of concurrent transactions submitted to the target system
        public int concurrencyLevel { get; set; }

        // a timer is configured to notify the orchestrator grain about the termination
        public int executionTime { get; set; } = 6000;

        public int delayBetweenRequests { get; set; } = 1000;

        /**
         * from highest to lowest. last entry must be 100
         * e.g. customer_session 70, price_update 95, delivery 100
         * that means 70% of transactions are customer sessions
         * 25% price update and 5% delivery
         */
        public IDictionary<TransactionType,int> transactionDistribution { get; set; }

        public StreamingConfig streamingConfig { get; set; }

        public CustomerWorkerConfig customerWorkerConfig { get; set; }

        public SellerWorkerConfig sellerWorkerConfig { get; set; }

        public DeliveryWorkerConfig deliveryWorkerConfig { get; set; }

        // customer key distribution
        public DistributionType customerDistribution { get; set; }

    }
}
