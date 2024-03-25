using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Workload;
using Orleans.Workers;
using Orleans.Metric;
using DuckDB.NET.Data;
using Common.Workers.Delivery;
using Common.Metric;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Orleans.Workload;

public sealed class ActorExperimentManager : AbstractExperimentManager
{
    private readonly ActorWorkloadManager workloadManager;
    private readonly ActorMetricManager metricManager;

    public static ActorExperimentManager BuildActorExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection connection)
    {
        return new ActorExperimentManager(httpClientFactory, ActorSellerWorker.BuildSellerWorker, ActorCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private ActorExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection connection) :
        base(httpClientFactory, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, connection)
    {
        this.workloadManager = new ActorWorkloadManager(
            this.sellerService, this.customerService, this.deliveryService,
            config.transactionDistribution,
            // set in the base class
            this.customerRange,
            config.concurrencyLevel,
            config.executionTime,
            config.delayBetweenRequests);

        this.metricManager = new ActorMetricManager(this.sellerService, this.customerService, this.deliveryService);
    }

    public async Task RunSimpleExperiment(int type)
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.SetUpWorkloadManager(0);
        (DateTime startTime, DateTime finishTime) res;
        if(type == 0){
            Console.WriteLine("Thread mode selected.");
            res = this.workloadManager.RunThreads();
        }
        else if(type == 1) {
            Console.WriteLine("Task mode selected.");
            res = this.workloadManager.RunTasks();
        }
        else {
            Console.WriteLine("Task per Tx mode selected.");
            res = await this.workloadManager.RunTaskPerTx();
        }
        DateTime startTime = res.startTime;
        DateTime finishTime = res.finishTime;
        this.Collect(0, startTime, finishTime);
        this.PostExperiment();
        this.CollectGarbage();
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
