using Common.Distribution;
using Common.Entities;
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
    protected Entities.Customer customer;

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

    public void SetDistribution(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        this.sellerIdGenerator = sellerDistribution == DistributionType.UNIFORM ?
                                  new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                  new Zipf(0.80, sellerRange.max, new Random());
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(1, numberOfProducts, new Random()) :
                                new Zipf(0.99, numberOfProducts, new Random());
    }

    public void Run(int tid)
    {
        AddItemsToCart();
        Checkout(tid);
    }

    protected abstract void AddItem(ISet<(int,int)> addedSet);

    public void AddItemsToCart()
    {
        int numberOfProducts = random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
        ISet<(int, int)> set = new HashSet<(int, int)>();
        while (set.Count < numberOfProducts)
        {
            AddItem(set);
        }
    }

    public void Checkout(int tid)
    {
        // define whether client should send a checkout request
        if (random.Next(0, 100) > this.config.checkoutProbability)
        {
            InformFailedCheckout();
            return;
        }
        SendCheckoutRequest(tid);
    }

    protected abstract void SendCheckoutRequest(int tid);

    // i.e., seal the cart
    protected abstract void InformFailedCheckout();

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.submittedTransactions;
    }

    public abstract List<TransactionOutput> GetFinishedTransactions();
}