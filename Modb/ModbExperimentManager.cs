using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Metric;
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

    public static ModbExperimentManager BuildModbExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        return new ModbExperimentManager(httpClientFactory, DefaultSellerWorker.BuildSellerWorker, ModbCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, duckDBConnection);
    }

    public ModbExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, duckDBConnection)
    {

    }

    public async Task RunSimpleExperiment(Func<int> callback)
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.workloadManager.SetUp(this.config.runs[0].sellerDistribution, new Interval(1, this.numSellers));
        (DateTime startTime, DateTime finishTime) = await this.workloadManager.Run();

        MetricManager.SimpleCollect(startTime, finishTime, callback);

        this.PostRunTasks(0);
        this.PostExperiment();
        this.CollectGarbage();
    }

}

