using Common.Services;
using Common.Workload;

namespace Dapr.Workload;

public sealed class DaprWorkloadManager : WorkloadManager
{
    public DaprWorkloadManager(
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(sellerService, customerService, deliveryService, transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests){ }

}