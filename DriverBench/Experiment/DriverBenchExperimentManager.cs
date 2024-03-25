using Common.Experiment;
using Common.Metric;
using Common.Workload;
using DriverBench.Workers;
using DuckDB.NET.Data;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace DriverBench.Experiment;

public sealed class DriverBenchExperimentManager : AbstractExperimentManager
{

    private readonly WorkloadManager workloadManager;
    private readonly MetricManager metricManager;

    public static DriverBenchExperimentManager BuildDriverBenchExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        return new DriverBenchExperimentManager(httpClientFactory, SellerWorker.BuildSellerWorker, CustomerWorker.BuildCustomerWorker, DeliveryWorker.BuildDeliveryWorker, config, duckDBConnection);
    }

    public DriverBenchExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection) : base(httpClientFactory, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, duckDBConnection)
    {
        this.workloadManager = new WorkloadManager(
            this.sellerService, this.customerService, this.deliveryService,
            config.transactionDistribution,
            this.customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);

        this.metricManager = new MetricManager(this.sellerService, this.customerService, this.deliveryService);
    }

    protected override WorkloadManager SetUpWorkloadManager(int runIdx)
    {
        this.workloadManager.SetUp(this.config.runs[runIdx].sellerDistribution, new Interval(1, this.numSellers));
        return this.workloadManager;
    }

    protected override MetricManager SetUpMetricManager(int runIdx)
    {
        this.metricManager.SetUp(this.numSellers, this.config.numCustomers);
        return this.metricManager;
    }



}

