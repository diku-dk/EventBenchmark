using System;
using System.Threading.Tasks;
using Common.Entities;
using Common.Experiment;
using Common.Infra;
using Common.Metric;
using Common.Workers.Delivery;
using Common.Workload;
using DuckDB.NET.Data;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Monitor;

public class MonitorExperimentManager : AbstractExperimentManager
{

    public static MonitorExperimentManager BuildExperimentManager(ExperimentConfig config, DuckDBConnection connection)
    {
        return new MonitorExperimentManager(null, MonitorCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, connection);
    }

    private MonitorExperimentManager(BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection connection) :
        base(null, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, connection) { }

    public void RunMonitorExperiment()
    {
        // TODO set up the threads for synthetic actors
        int numCartActors = 10;
        for(int i = 0; i < numCartActors; i++)
        {
            // creates thread for actor
             Task.Run(() => {
                 //  
                 var mailbox = ((MonitorCustomerWorker)customerWorkers[i]).mailbox;
                 var cartActor = new CartActor(mailbox, null, new CartActorConfig());
                 cartActor.Run();
             });
        }

        // TODO do this for all synthetic actor we create
        // cart, stock, order, payment, shipment

        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.workloadManager.SetUp(new Interval(1, this.numSellers), this.config.runs[0].sellerDistribution, this.config.runs[0].sellerZipfian);
        (DateTime startTime, DateTime finishTime) = this.workloadManager.Run();
        this.Collect(0, startTime, finishTime);
        this.PostRunTasks(0);
        this.PostExperiment();
        CollectGarbage();
    }

}