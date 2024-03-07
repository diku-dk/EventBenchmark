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

    private readonly ISet<string> tids;

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
        this.tids.Clear();
    }

    /**
     * This method expects the content to contain items belonging to the cart
     */
    protected override void DoAfterSubmission(string tid)
    {
        this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));

        if (this.config.trackReplication)
        {
            // store
            // i dont need to current state. I need to get the cart items state right before submission to checkout
            // var clonedCartItems = new Dictionary<(int, int),Product>(this.cartItems);
            this.tids.Add(tid);
        }

    }



}

