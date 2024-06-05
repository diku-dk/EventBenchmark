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
    private CancellationTokenSource cancellationTokenSource;

    // define a pulling thread list which contains 3 pulling threads
    private readonly List<StatefunPollingThread> receiptPullingThreads;

    private static readonly int NUM_PULLING_THREADS = 3;

    public static StatefunExperimentManager BuildStatefunExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection)
    {
        return new StatefunExperimentManager(httpClientFactory, StatefunSellerWorker.BuildSellerWorker, StatefunCustomerWorker.BuildCustomerWorker, StatefunDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private StatefunExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection connection = null) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, connection)
    {
        this.receiptPullingThreads = new List<StatefunPollingThread>();
    }

    protected override void PreExperiment()
    {
        base.PreExperiment();
        for (int i = 0; i < NUM_PULLING_THREADS; i++)
        {
            this.receiptPullingThreads.Add(new StatefunPollingThread(this.config.pollingUrl, this.config.pollingRate, this.customerService, this.sellerService, this.deliveryService));
        }

        // start pulling threads to collect receipts
        this.cancellationTokenSource = new CancellationTokenSource();
        foreach (var thread in this.receiptPullingThreads)
        {
            Task.Factory.StartNew(() => thread.Run(cancellationTokenSource.Token), TaskCreationOptions.LongRunning);
        }
        Console.WriteLine("=== Starting receipt pulling thread ===");
    }

    protected override void PostExperiment()
    {
        base.PostExperiment();
        this.cancellationTokenSource.Cancel();
        this.receiptPullingThreads.Clear();
    }

}

