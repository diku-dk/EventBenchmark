using Common.Experiment;
using Common.Metric;
using Common.Workers.Customer;
using Common.Workers.Delivery;
using Common.Workers.Seller;
using Common.Workload;
using DuckDB.NET.Data;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Modb;

public sealed class ModbExperimentManager : AbstractExperimentManager
{
    private readonly MetricManager metricManager;

    public static ModbExperimentManager BuildModbExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        return new ModbExperimentManager(httpClientFactory, DefaultSellerWorker.BuildSellerWorker, DefaultCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, duckDBConnection);
    }

    public ModbExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, duckDBConnection)
    {
        this.metricManager = new MetricManager(this.sellerService, this.customerService, this.deliveryService);
    }

    protected override MetricManager SetUpMetricManager(int runIdx)
    {
        this.metricManager.SetUp(this.numSellers, this.config.numCustomers);
        return this.metricManager;
    }

}

