using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;
using Microsoft.Extensions.Logging;

namespace Statefun.Metric;

public class StatefunMetricManager : MetricManager
{
    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;
    private int numSellers;
    private int numCustomers;

    public StatefunMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base()
	{
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
	}

    public void SetUp(int numSellers, int numCustomers)
    {
        this.numSellers = numSellers;
        this.numCustomers = numCustomers;
    }

    protected override Dictionary<TransactionType, int> CollectAborts(DateTime finishTime)
    {
        Dictionary<TransactionType, int> abortCount = new()
        {
            { TransactionType.PRICE_UPDATE, 0 },
             { TransactionType.UPDATE_PRODUCT, 0 },
              { TransactionType.CUSTOMER_SESSION, 0 },
                { TransactionType.QUERY_DASHBOARD, 0 },
                  { TransactionType.UPDATE_DELIVERY, 0 },
        };
        var sellerAborts = sellerService.GetAbortedTransactions();
        foreach(var abort in sellerAborts){
            abortCount[abort.type]++;
        }

        var customerAborts = customerService.GetAbortedTransactions();
        abortCount[TransactionType.CUSTOMER_SESSION] += customerAborts.Count;

        var deliveryAborts = deliveryService.GetAbortedTransactions();
        abortCount[TransactionType.UPDATE_DELIVERY] += deliveryAborts.Count;
        
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
            var finished = this.customerService.GetFinishedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!customerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    logger.LogDebug("[Customer] Duplicate submitted transaction entry found. Existing {0} New {1} ", customerSubmitted[tx.tid], tx);
                }
            }

            foreach (var tx in finished)
            {
                if (!customerFinished.TryAdd(tx.tid, tx))
                {
                    dupFin++;
                    logger.LogDebug("[Customer] Duplicate finished transaction entry found. Existing {0} New {1} ", customerFinished[tx.tid], tx);
                }
            }

        }

        if (dupSub > 0)
            logger.LogWarning("[Customer] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Customer] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(customerSubmitted, customerFinished, finishTime, "customer");

    }

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

        if (dupSub > 0)
            logger.LogWarning("[Seller] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Seller] Number of duplicated finished transactions found: {0}", dupFin);

         return BuildLatencyList(sellerSubmitted, sellerFinished, finishTime, "seller");
    }
}