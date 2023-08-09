using Common.Distribution;
using Common.Workload;
using Daprr.Services;

namespace Daprr.Workload;

public class DaprWorkflowManager : WorkloadManager
{

    private readonly ISellerService sellerService;
    private readonly ICustomerService customerService;
    private readonly IDeliveryService deliveryService;

    public DaprWorkflowManager(
        ISellerService sellerService, ICustomerService customerService,
        IDeliveryService deliveryService,
        IDictionary<TransactionType, int> transactionDistribution,
        DistributionType sellerDistribution, Interval sellerRange,
        DistributionType customerDistribution, Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(transactionDistribution, sellerDistribution, sellerRange, customerDistribution,
        customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
    }

    protected override void SubmitTransaction(int tid, TransactionType type)
    {
        try
        {
            switch (type)
            {
                case TransactionType.CUSTOMER_SESSION:
                    {
                        int customerId = this.keyGeneratorPerWorkloadType[type].Sample();
                        // make sure there is no active session for the customer. if so, pick another customer
                        int count = 1;
                        while (!customerStatusCache.TryUpdate(customerId, WorkerStatus.ACTIVE, WorkerStatus.IDLE))
                        {
                            if (count > 5)
                            {
                                logger.LogWarning("[Workload emitter] Could not find an available customer! Perhaps should increase the number of customer next time?");
                                while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
                                return;
                            }
                            customerId = this.keyGeneratorPerWorkloadType[type].Sample();
                            count++;
                        }

                        Task.Run(() => customerService.Run(customerId, tid)).ContinueWith(x => this.customerStatusCache[customerId] = WorkerStatus.IDLE);
                        break;
                    }
                // delivery worker
                case TransactionType.UPDATE_DELIVERY:
                    {
                        Task.Run(() => deliveryService.Run(tid)).ContinueWith(x => Shared.ResultQueue.Writer.WriteAsync(ITEM));
                        break;
                    }
                // seller worker
                case TransactionType.DASHBOARD:
                    {
                        int sellerId = this.keyGeneratorPerWorkloadType[type].Sample();
                        Task.Run(() => sellerService.Run(sellerId, tid, type)).ContinueWith(x =>
                        {
                            this.sellerStatusCache[sellerId] = WorkerStatus.IDLE;
                            while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
                        });
                        break;
                    }
                case TransactionType.PRICE_UPDATE:
                case TransactionType.DELETE_PRODUCT:
                    {
                        int sellerId = this.keyGeneratorPerWorkloadType[type].Sample();

                        int count = 1;
                        while (!sellerService.HasAvailableProduct(sellerId) || sellerStatusCache[sellerId] == WorkerStatus.ACTIVE || !sellerStatusCache.TryUpdate(sellerId, WorkerStatus.ACTIVE, WorkerStatus.IDLE))
                        {
                            if (count > 5)
                            {
                                logger.LogDebug("[Workload emitter] A lot of sellers seem busy or without products. Aborting seller operation!");
                                while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
                                break;
                            }
                            sellerId = this.keyGeneratorPerWorkloadType[type].Sample();
                            count++;
                        }
   
                        Task.Run(() => sellerService.Run(sellerId, tid, type)).
                            ContinueWith(x => this.sellerStatusCache[sellerId] = WorkerStatus.IDLE);
                        
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