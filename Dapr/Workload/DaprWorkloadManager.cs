using Common.Services;
using Common.Workload;

namespace Dapr.Workload;

public class DaprWorkloadManager : WorkloadManager
{

    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;

    public DaprWorkloadManager(
        ISellerService sellerService,
        ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(transactionDistribution, customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
    }

    protected override void SubmitTransaction(string tid, TransactionType type)
    {
        try
        {
            switch (type)
            {
                case TransactionType.CUSTOMER_SESSION:
                    {
                        int customerId = this.customerIdleQueue.Take();
                        Task.Run(() => customerService.Run(customerId, tid)).ContinueWith(x => this.customerIdleQueue.Add(customerId));
                        break;
                    }
                // delivery worker
                case TransactionType.UPDATE_DELIVERY:
                    {
                        Task.Run(() => deliveryService.Run(tid)).ContinueWith(async x => await Shared.ResultQueue.Writer.WriteAsync(ITEM));
                        break;
                    }
                // seller worker
                case TransactionType.QUERY_DASHBOARD:
                    {
                        int sellerId = this.sellerIdGenerator.Sample();
                        Task.Run(() => sellerService.Run(sellerId, tid, type)).ContinueWith(async x => await Shared.ResultQueue.Writer.WriteAsync(ITEM));
                        break;
                    }
                case TransactionType.PRICE_UPDATE:
                case TransactionType.UPDATE_PRODUCT:
                    {
                        int sellerId = this.sellerIdGenerator.Sample();
                        Task.Run(() => sellerService.Run(sellerId, tid, type));
                        break;
                    }
                default:
                    {
                        long threadId = Environment.CurrentManagedThreadId;
                        this.logger.LogError("Thread ID " + threadId + " Unknown transaction type defined!");
                        break;
                    }
            }
        }
        catch (Exception e)
        {
            long threadId = Environment.CurrentManagedThreadId;
            this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
        }
    }

}