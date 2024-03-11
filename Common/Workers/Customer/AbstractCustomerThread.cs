using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using MathNet.Numerics.Distributions;
using Microsoft.Extensions.Logging;
using Common.Workload.CustomerWorker;
using Common.Streaming;
using Common.Entities;

namespace Common.Workers.Customer;

public abstract class AbstractCustomerThread : ICustomerWorker
{  
    protected readonly Random random;

    protected CustomerWorkerConfig config;

    protected IDiscreteDistribution sellerIdGenerator;
    protected readonly int numberOfProducts;
    protected IDiscreteDistribution productIdGenerator;

    // the object respective to this worker
    protected readonly Entities.Customer customer;

    // not concurrent because it is not necessary
    protected readonly List<TransactionIdentifier> submittedTransactions;

    protected readonly List<TransactionMark> abortedTransactions;

    protected readonly ISellerService sellerService;

    protected readonly ILogger logger;

    protected AbstractCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Entities.Customer customer, ILogger logger)
    {
        this.sellerService = sellerService;
        this.config = config;
        this.customer = customer;
        this.numberOfProducts = numberOfProducts;
        this.logger = logger;
        this.submittedTransactions = new();
        this.abortedTransactions = new();
        this.random = new Random();
    }

    public virtual void SetUp(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        this.sellerIdGenerator = sellerDistribution == DistributionType.UNIFORM ?
                                  new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                  new Zipf(WorkloadConfig.sellerZipfian, sellerRange.max, new Random());
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(1, numberOfProducts, new Random()) :
                                new Zipf(WorkloadConfig.productZipfian, numberOfProducts, new Random());

        this.submittedTransactions.Clear();
    }

    public void Run(string tid)
    {
        this.AddItemsToCart();
        this.Checkout(tid);
    }

    public abstract void AddItemsToCart();

    public void Checkout(string tid)
    {
        // define whether client should send a checkout request
        if (this.random.Next(0, 101) > this.config.checkoutProbability)
        {
            this.InformFailedCheckout();
            return;
        }
        this.SendCheckoutRequest(tid);
    }

    protected abstract void SendCheckoutRequest(string tid);

    // i.e., seal the cart
    protected abstract void InformFailedCheckout();

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.submittedTransactions;
    }

    public virtual void AddFinishedTransaction(TransactionOutput transactionOutput)
    {
        throw new NotImplementedException();
    }

    public List<TransactionMark> GetAbortedTransactions()
    {
        return this.abortedTransactions;
    }

    public abstract List<TransactionOutput> GetFinishedTransactions();

    public virtual IDictionary<string, List<CartItem>> GetCartItemsPerTid(DateTime finishTime)
    {
        throw new NotImplementedException();
    }

}