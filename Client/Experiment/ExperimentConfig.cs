using System.Collections.Generic;
using Client.Ingestion.Config;
using Client.Streaming;
using Common.Workload;
using Common.Workload.Customer;
using Common.Workload.Delivery;
using Common.Workload.Seller;

namespace Client.Experiment
{
	public class ExperimentConfig
	{

        public bool enabled { get; set; }

        public string connectionString { get; set; }

        public int numCustomers { get; set; }
        public int avgNumProdPerSeller { get; set; }

        public IngestionConfig ingestionConfig { get; set; }

        public List<RunConfig> runs { get; set; }

        public List<PostRunTask> postRunTasks { get; set; }

        public IDictionary<TransactionType, int> transactionDistribution { get; set; }

        public int executionTime { get; set; }

        public int delayBetweenRequests { get; set; }

        public int concurrencyLevel { get; set; }

        public StreamingConfig streamingConfig { get; set; }

        public CustomerWorkerConfig customerWorkerConfig { get; set; }

        public SellerWorkerConfig sellerWorkerConfig { get; set; }

        public DeliveryWorkerConfig deliveryWorkerConfig { get; set; }
    }
}

