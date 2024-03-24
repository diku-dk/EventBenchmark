using Common.Workers.Delivery;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace DriverBench.Workers;

public sealed class DeliveryWorker : DefaultDeliveryWorker
{
    public DeliveryWorker(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger) : base(config, httpClient, logger)
    {
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
