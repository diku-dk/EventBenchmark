using Common.Entities;
using Common.Http;
using Common.Services;
using Common.Workers.Customer;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace DriverBench.Workers;

public sealed class CustomerWorker : DefaultCustomerWorker
{
    private readonly List<TransactionOutput> finishedTransactions;

    public CustomerWorker(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, httpClient, logger)
    {
        this.finishedTransactions = new();
    }

    protected override void BuildAddCartPayloadAndSend(string objStr)
    {
        var payload = HttpUtils.BuildPayload(objStr);
        HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customer.id + "/add")
        {
            Content = payload
        };
        Thread.Sleep(100);
    }

    public override List<TransactionOutput> GetFinishedTransactions()
    {
        return this.finishedTransactions;
    }

    protected override void InformFailedCheckout()
    {
        Thread.Sleep(100);
    }

    protected override void SendCheckoutRequest(string tid)
    {
        this.submittedTransactions.Add(new TransactionIdentifier(tid, Common.Workload.TransactionType.CUSTOMER_SESSION, DateTime.UtcNow));
        // fixed delay
        Thread.Sleep(100);
        this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
    }
}
