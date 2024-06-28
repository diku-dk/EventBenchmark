using Common.Ingestion.Config;
using Common.Streaming;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Common.Workload.Delivery;
using Common.Workload.Seller;

namespace Common.Experiment;

public class ExperimentConfig
{
    // whether ingestion data is going to in disk or memory
    // use disk for experiments involving substantial amount of data
    public string connectionString { get; set; }

    public int numCustomers { get; set; }

    public int numProdPerSeller { get; set; }

    public int qtyPerProduct { get; set; }

    public IngestionConfig ingestionConfig { get; set; }

    public List<RunConfig> runs { get; set; }

    public List<PostRunTask> postRunTasks { get; set; }

    public List<PostRunTask> postExperimentTasks { get; set; }

    public int delayBetweenRuns { get; set; }

    public IDictionary<TransactionType, int> transactionDistribution { get; set; }

    public int executionTime { get; set; }

    public int epoch { get; set; }

    public int pollingRate { get; set; }

    public string pollingUrl { get; set; }

    public int delayBetweenRequests { get; set; }

    public int concurrencyLevel { get; set; }

    public ConcurrencyType concurrencyType { get; set; } = ConcurrencyType.CONTROL;

    public StreamingConfig streamingConfig { get; set; }

    public CustomerWorkerConfig customerWorkerConfig { get; set; }

    public SellerWorkerConfig sellerWorkerConfig { get; set; }

    public DeliveryWorkerConfig deliveryWorkerConfig { get; set; }
}

