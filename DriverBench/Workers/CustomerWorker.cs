using Common.Entities;
using Common.Services;
using Common.Workers.Customer;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace DriverBench.Workers;

public sealed class CustomerWorker : AbstractCustomerThread
{
    public CustomerWorker(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, ILogger logger) : base(sellerService, numberOfProducts, config, customer, logger)
    {
    }

    public override void AddItemsToCart()
    {
        // TODO add fixed delay
        throw new NotImplementedException();
    }

    public override List<TransactionOutput> GetFinishedTransactions()
    {
        throw new NotImplementedException();
    }

    protected override void InformFailedCheckout()
    {
        throw new NotImplementedException();
    }

    protected override void SendCheckoutRequest(string tid)
    {
        // TODO add fixed delay
        throw new NotImplementedException();
    }
}
