using Common.Distribution;
using Common.Entities;
using Common.Infra;
using Common.Services;
using Common.Workers.Customer;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Orleans.Workload;

public class ActorCustomerThread : HttpCustomerThread
{
    protected readonly List<TransactionOutput> finishedTransactions;

    private ActorCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, httpClient, logger)
    {
        this.finishedTransactions = new List<TransactionOutput>();
    }

    public static new ActorCustomerThread BuildCustomerThread(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
        return new ActorCustomerThread(sellerService, numberOfProducts, config, customer, httpClientFactory.CreateClient(), logger);
    }

    public override List<TransactionOutput> GetFinishedTransactions()
    {
        return this.finishedTransactions;
    }

    public override void SetUp(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        base.SetUp(sellerDistribution, sellerRange, keyDistribution);
        this.finishedTransactions.Clear();
    }

    protected override void SendCheckoutRequest(int tid)
    {
        var payload = BuildCheckoutPayload(tid);
        HttpRequestMessage message = new(HttpMethod.Post, this.config.cartUrl + "/" + this.customer.id + "/checkout")
        {
            Content = payload
        };

        var now = DateTime.UtcNow;
        HttpResponseMessage resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, now);
            this.submittedTransactions.Add(txId);
            this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
        }
        else
        {
            InformFailedCheckout();
        }
    }
}

