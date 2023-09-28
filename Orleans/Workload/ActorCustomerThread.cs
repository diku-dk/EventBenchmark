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

    protected override void DoAfterSubmission(string tid)
    {
        this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));      
    }

}

