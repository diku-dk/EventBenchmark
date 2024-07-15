using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;
using Common.Streaming;

namespace Daprr.Metric;

public sealed class DaprMetricManager : MetricManager
{

    DaprMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService)
	{
	}

    public static DaprMetricManager BuildDaprMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService)
    {
        return new DaprMetricManager(sellerService, customerService, deliveryService);
    }

    protected override Dictionary<TransactionType, int> CollectAborts(DateTime finishTime)
    {
        Dictionary<TransactionType, int> abortCount = base.CollectAborts(finishTime);

        while (Shared.PoisonPriceUpdateOutputs.Reader.TryRead(out _))
        {
            abortCount[TransactionType.PRICE_UPDATE]++;
        }

        while (Shared.PoisonProductUpdateOutputs.Reader.TryRead(out _))
        {
            abortCount[TransactionType.UPDATE_PRODUCT]++;
        }

        List<TransactionMark> customerAborts = new();
        while (Shared.PoisonCheckoutOutputs.Reader.TryRead(out var item))
        {
            customerAborts.Add(item);
            abortCount[TransactionType.CUSTOMER_SESSION]++;
        }

        if(customerAborts.Count > 0)
        {
            OutputCustomerAbortsAggregated(customerAborts);
        }

        return abortCount;
    }

    protected override List<Latency> CollectFromCustomer(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> customerSubmitted = new();
        Dictionary<object, TransactionOutput> customerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= this.numCustomers; i++)
        {
            var submitted = this.customerService.GetSubmittedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!customerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    LOGGER.LogDebug("[Customer] Duplicate submitted transaction entry found. Existing {0} New {1} ", customerSubmitted[tx.tid], tx);
                }
            }
        }

        while (Shared.CheckoutOutputs.Reader.TryRead(out TransactionOutput item))
        {
            if (!customerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                LOGGER.LogDebug("[Customer] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        if (dupSub > 0)
            LOGGER.LogWarning("[Customer] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            LOGGER.LogWarning("[Customer] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(customerSubmitted, customerFinished, finishTime, "customer");
    }

    protected override List<Latency> CollectFromSeller(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> sellerSubmitted = new();
        Dictionary<object, TransactionOutput> sellerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= this.numSellers; i++)
        {
            var submitted = this.sellerService.GetSubmittedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!sellerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    LOGGER.LogDebug("[Seller] Duplicate submitted transaction entry found. Existing {0} New {1} ", sellerSubmitted[tx.tid], tx);
                }
            }

            var finished = this.sellerService.GetFinishedTransactions(i);
            foreach (var tx in finished)
            {
                if (!sellerFinished.TryAdd(tx.tid, tx))
                {
                    dupFin++;
                    LOGGER.LogDebug("[Seller] Duplicate finished transaction entry found. Existing {0} New {1} ", sellerFinished[tx.tid], finished);
                }
            }
        }

        TransactionOutput item;
        while (Shared.ProductUpdateOutputs.Reader.TryRead(out item))
        {
            if (!sellerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                LOGGER.LogDebug("[Seller] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        while (Shared.PriceUpdateOutputs.Reader.TryRead(out item))
        {
            if (!sellerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                LOGGER.LogDebug("[Seller] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        if (dupSub > 0)
            LOGGER.LogWarning("[Seller] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            LOGGER.LogWarning("[Seller] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(sellerSubmitted, sellerFinished, finishTime, "seller");
    }
}


