using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;

namespace Daprr.Metric;

public sealed class DaprMetricManager : MetricManager
{

    public DaprMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base(sellerService, customerService, deliveryService)
	{
	}

    protected override Dictionary<TransactionType, int> CollectAborts(DateTime finishTime)
    {
        Dictionary<TransactionType, int> abortCount = new()
        {
            { TransactionType.PRICE_UPDATE, 0 },
             { TransactionType.UPDATE_PRODUCT, 0 },
              { TransactionType.CUSTOMER_SESSION, 0 },
        };
        while (Shared.PoisonPriceUpdateOutputs.Reader.TryRead(out _))
        {
            abortCount[TransactionType.PRICE_UPDATE]++;
        }

        while (Shared.PoisonProductUpdateOutputs.Reader.TryRead(out _))
        {
            abortCount[TransactionType.UPDATE_PRODUCT]++;
        }

        while (Shared.PoisonCheckoutOutputs.Reader.TryRead(out _))
        {
            abortCount[TransactionType.CUSTOMER_SESSION]++;
        }
        return abortCount;
    }

    protected override List<Latency> CollectFromCustomer(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> customerSubmitted = new();
        Dictionary<object, TransactionOutput> customerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= numCustomers; i++)
        {
            var submitted = this.customerService.GetSubmittedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!customerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    logger.LogDebug("[Customer] Duplicate submitted transaction entry found. Existing {0} New {1} ", customerSubmitted[tx.tid], tx);
                }
            }
        }

        while (Shared.CheckoutOutputs.Reader.TryRead(out TransactionOutput item))
        {
            if (!customerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                logger.LogDebug("[Customer] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Customer] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Customer] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(customerSubmitted, customerFinished, finishTime, "customer");
    }

    public override List<Latency> CollectFromDelivery(DateTime finishTime)
    {
        int dupSub = 0;
        int dupFin = 0;

        var res = this.deliveryService.GetResults();
        Dictionary<object, TransactionIdentifier> deliverySubmitted = new();
        Dictionary<object, TransactionOutput> deliveryFinished = new();
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

    protected override List<Latency> CollectFromSeller(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> sellerSubmitted = new();
        Dictionary<object, TransactionOutput> sellerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= numSellers; i++)
        {
            var submitted = this.sellerService.GetSubmittedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!sellerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    logger.LogDebug("[Seller] Duplicate submitted transaction entry found. Existing {0} New {1} ", sellerSubmitted[tx.tid], tx);
                }
            }

            var finished = this.sellerService.GetFinishedTransactions(i);
            foreach (var tx in finished)
            {
                if (!sellerFinished.TryAdd(tx.tid, tx))
                {
                    dupFin++;
                    logger.LogDebug("[Seller] Duplicate finished transaction entry found. Existing {0} New {1} ", sellerFinished[tx.tid], finished);
                }
            }
        }

        TransactionOutput item;
        while (Shared.ProductUpdateOutputs.Reader.TryRead(out item))
        {
            if (!sellerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                logger.LogDebug("[Seller] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        while (Shared.PriceUpdateOutputs.Reader.TryRead(out item))
        {
            if (!sellerFinished.TryAdd(item.tid, item))
            {
                dupFin++;
                logger.LogDebug("[Seller] Duplicate finished transaction entry found: Tid {0} Mark {1}", item.tid, item);
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Seller] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Seller] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(sellerSubmitted, sellerFinished, finishTime, "seller");

    }
}


