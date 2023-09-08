using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using Common.Metric;
using Microsoft.Extensions.Logging;

namespace Orleans.Metric;

public class ActorMetricManager : MetricManager
{

    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;
    private int numSellers;
    private int numCustomers;

    public ActorMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService) : base()
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
        throw new NotImplementedException();
    }

    protected override List<Latency> CollectFromCustomer(DateTime finishTime)
    {
        Dictionary<int, TransactionIdentifier> customerSubmitted = new();
        Dictionary<int, TransactionOutput> customerFinished = new();

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
        }

        return null;

    }

    protected override List<Latency> CollectFromDelivery(DateTime finishTime)
    {
        throw new NotImplementedException();
    }

    protected override List<Latency> CollectFromSeller(DateTime finishTime)
    {
        throw new NotImplementedException();
    }
}