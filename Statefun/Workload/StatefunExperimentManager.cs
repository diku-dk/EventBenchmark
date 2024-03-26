using Common.Experiment;
using Common.Workload;
using DuckDB.NET.Data;
using Common.Metric;
using Statefun.Workers;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Statefun.Workload;

public sealed class StatefunExperimentManager : AbstractExperimentManager
{        
    private static readonly string RECEIPT_URL = "http://statefunhost:8091/receipts";

    private CancellationTokenSource cancellationTokenSource;

    private readonly MetricManager metricManager;

    // define a pulling thread list which contains 3 pulling threads
    private readonly List<StatefunReceiptPullingThread> receiptPullingThreads;

    private static readonly int NUM_PULLING_THREADS = 3;

    public static StatefunExperimentManager BuildStatefunExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection)
    {
        return new StatefunExperimentManager(httpClientFactory, StatefunSellerWorker.BuildSellerWorker, StatefunCustomerWorker.BuildCustomerWorker, StatefunDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private StatefunExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection connection = null) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, connection)
    {
        this.metricManager = new MetricManager(this.sellerService, this.customerService, this.deliveryService);
        this.receiptPullingThreads = new List<StatefunReceiptPullingThread>();
    }

    protected override void PreExperiment()
    {
        base.PreExperiment();
        for (int i = 0; i < NUM_PULLING_THREADS; i++)
        {
            this.receiptPullingThreads.Add(new StatefunReceiptPullingThread(RECEIPT_URL, this.customerService, this.sellerService, this.deliveryService));
        }

        // start pulling threads to collect receipts
        this.cancellationTokenSource = new CancellationTokenSource();
        foreach (var thread in this.receiptPullingThreads)
        {
            Task.Factory.StartNew(() => thread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }
        Console.WriteLine("=== Starting receipt pulling thread ===");
    }

    protected override MetricManager SetUpMetricManager(int runIdx)
    {
        this.metricManager.SetUp(this.numSellers, this.config.numCustomers);
        return this.metricManager;
    }

    protected override void PostExperiment()
    {
        base.PostExperiment();
        this.cancellationTokenSource.Cancel();
        this.receiptPullingThreads.Clear();
    }

}

