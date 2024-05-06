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

    public static DriverBenchExperimentManager BuildDriverBenchExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        return new DriverBenchExperimentManager(httpClientFactory, DriverBenchSellerWorker.BuildSellerWorker, DriverBenchCustomerWorker.BuildCustomerWorker, DriverBenchDeliveryWorker.BuildDeliveryWorker, config, duckDBConnection);
    }

    private DriverBenchExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, duckDBConnection)
    {
        
    }

}

