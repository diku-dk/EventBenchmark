using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;
using Microsoft.Extensions.Logging;

namespace Statefun.Metric;

public sealed class StatefunMetricManager : MetricManager
{

    public StatefunMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService) { }

    protected override List<Latency> CollectFromDelivery(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> deliverySubmitted = new();
        Dictionary<object, TransactionOutput> deliveryFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        var submitted = this.deliveryService.GetSubmittedTransactions();        
        foreach (var tx in submitted)
        {
            if (!deliverySubmitted.TryAdd(tx.tid, tx))
            {
                dupSub++;
                logger.LogDebug("[Delivery] Duplicate submitted transaction entry found. Existing {0} New {1} ", deliverySubmitted[tx.tid], tx);
            }
        }

        var finished = this.deliveryService.GetFinishedTransactions();
        foreach (var tx in finished)
        {
            if (!deliveryFinished.TryAdd(tx.tid, tx))
            {
                dupFin++;
                logger.LogDebug("[Delivery] Duplicate finished transaction entry found. Existing {0} New {1} ", deliveryFinished[tx.tid], tx);
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Delivery] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Delivery] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(deliverySubmitted, deliveryFinished, finishTime, "delivery");
    }

}