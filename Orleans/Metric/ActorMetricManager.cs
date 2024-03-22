using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;
using Microsoft.Extensions.Logging;

namespace Orleans.Metric;

public sealed class ActorMetricManager : MetricManager
{

    public ActorMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService) { }

    public override List<Latency> CollectFromDelivery(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> deliverySubmitted = new();
        Dictionary<object, TransactionOutput> deliveryFinished = new();
        int dupSub = 0;
        int dupFin = 0;

        var res = this.deliveryService.GetResults();
        
        foreach (var pair in res)
        {
            if (!deliverySubmitted.TryAdd(pair.Item1.tid, pair.Item1))
            {
                dupSub++;
            }
            if (!deliveryFinished.TryAdd(pair.Item2.tid, pair.Item2))
            {
                dupFin++;
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Delivery] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Delivery] Number of duplicated finished transactions found: {0}", dupFin);

        return this.BuildLatencyList(deliverySubmitted, deliveryFinished, finishTime, "delivery");
    }

}