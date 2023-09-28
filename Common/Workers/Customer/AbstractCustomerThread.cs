using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using MathNet.Numerics.Distributions;
using Microsoft.Extensions.Logging;
using Common.Workload.CustomerWorker;

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

    protected readonly List<TransactionIdentifier> submittedTransactions;

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
        this.random = new Random();
    }

    public virtual void SetUp(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        this.sellerIdGenerator = sellerDistribution == DistributionType.UNIFORM ?
                                  new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                  new Zipf(0.80, sellerRange.max, new Random());
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(1, numberOfProducts, new Random()) :
                                new Zipf(0.99, numberOfProducts, new Random());

        this.submittedTransactions.Clear();
    }

    public void Run(string tid)
    {
        //var threadId =  Environment.CurrentManagedThreadId;
        //logger.LogWarning("I am thread {0} with TID {1}", threadId, tid);
        AddItemsToCart();
        //logger.LogWarning("I am thread {0} with TID {1} finished adding items to cart!", threadId, tid);
        Checkout(tid);
        //logger.LogWarning("I am thread {0} with TID {1} finished checkout!", threadId, tid);
    }

    public abstract void AddItemsToCart();

    public void Checkout(string tid)
    {
        // define whether client should send a checkout request
        if (random.Next(0, 100) > this.config.checkoutProbability)
        {
            InformFailedCheckout();
            return;
        }
        SendCheckoutRequest(tid);
    }

    protected abstract void SendCheckoutRequest(string tid);

    // i.e., seal the cart
    protected abstract void InformFailedCheckout();

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.submittedTransactions;
    }

    public abstract List<TransactionOutput> GetFinishedTransactions();
}