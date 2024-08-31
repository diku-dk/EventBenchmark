using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Common.Entities;
using Common.Experiment;
using Common.Http;
using Common.Infra;
using Common.Metric;
using Common.Workers.Delivery;
using Common.Workers.Seller;
using Common.Workload;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using static Common.Services.CustomerService;
using static Common.Services.DeliveryService;
using static Common.Services.SellerService;

namespace Modb;

public sealed class ModbExperimentManager : AbstractExperimentManager
{
    private readonly ModbPollingTask modbPollingTask;

    public static ModbExperimentManager BuildModbExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        return new ModbExperimentManager(httpClientFactory, DefaultSellerWorker.BuildSellerWorker, ModbCustomerWorker.BuildCustomerWorker, DefaultDeliveryWorker.BuildDeliveryWorker, config, duckDBConnection);
    }

    public ModbExperimentManager(IHttpClientFactory httpClientFactory, BuildSellerWorkerDelegate sellerWorkerDelegate, BuildCustomerWorkerDelegate customerWorkerDelegate, BuildDeliveryWorkerDelegate deliveryWorkerDelegate, ExperimentConfig config, DuckDBConnection duckDBConnection) : base(httpClientFactory, WorkloadManager.BuildWorkloadManager, MetricManager.BuildMetricManager, sellerWorkerDelegate, customerWorkerDelegate, deliveryWorkerDelegate, config, duckDBConnection)
    {
        // must be at least same as batch window in modb
        this.modbPollingTask = new ModbPollingTask(this.config.pollingUrl, this.config.pollingRate);
    }

    public new void RunSimpleExperiment()
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        this.workloadManager.SetUp(this.config.runs[0].sellerDistribution, new Interval(1, this.numSellers));

        var tokenSource = new CancellationTokenSource();
        Task<long> task = Task.Run(() => this.modbPollingTask.Run(tokenSource.Token));

        // let first TID be polled
        Thread.Sleep(1);
        
        (DateTime startTime, DateTime finishTime) = this.workloadManager.Run(tokenSource);
       
        // wait for completion
        while(!task.IsCompleted){ }

        MetricManager.SimpleCollect(startTime, finishTime, task.Result);

        // this.PostRunTasks(0);

        if (this.WaitCompletion())
        {
            this.PostExperiment();
        }

        CollectGarbage();
    }

    private bool WaitCompletion()
    {
        var url = this.config.pollingUrl + "/status/submitted";
        int maxAttempts = 20;
        long lastCommittedTid;
        long lastSubmittedTid;
        do
        {
            Thread.Sleep(1000);
            lastSubmittedTid = PollLastSubmittedTid(url);
            LOGGER.LogInformation($"Last submitted TID retrieved: {lastSubmittedTid}");
            lastCommittedTid = this.modbPollingTask.PollLastCommittedTid();
            LOGGER.LogInformation($"Last committed TID retrieved: {lastCommittedTid}");
            maxAttempts--;
        } while (lastCommittedTid != lastSubmittedTid && maxAttempts > 0);
        // very weird that even after proxy confirming commit, there is task running in cart VMS...
        Thread.Sleep(2000);
        if(lastCommittedTid == lastSubmittedTid) return true;
        return false;
    }

    public static long PollLastSubmittedTid(string url)
    {
        HttpResponseMessage response = HttpUtils.client.Send(new(HttpMethod.Get, url));
        if(!response.IsSuccessStatusCode)
        {
            return -1;
        }
        byte[] ba = response.Content.ReadAsByteArrayAsync().Result;
        return BitConverter.ToInt64(ba);
    }

}

