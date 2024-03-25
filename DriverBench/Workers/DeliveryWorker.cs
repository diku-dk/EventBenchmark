using Common.Infra;
using Common.Workers.Delivery;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace DriverBench.Workers;

public sealed class DeliveryWorker : DefaultDeliveryWorker
{
    private DeliveryWorker(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger) : base(config, httpClient, logger)
    {
    }

    public static new DeliveryWorker BuildDeliveryWorker(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new DeliveryWorker(config, httpClientFactory.CreateClient(), logger);
    }

    public new void Run(string tid)
    {
        var start = new TransactionIdentifier(tid, Common.Workload.TransactionType.CUSTOMER_SESSION, DateTime.UtcNow);
        // fixed delay
        Thread.Sleep(100);
        var end = new TransactionOutput(tid, DateTime.UtcNow);
        this.resultQueue.Add((start,end));
    }

}
