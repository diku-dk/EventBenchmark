using System.Collections.Concurrent;
using Common.Distribution;
using Common.Entities;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using MathNet.Numerics.Distributions;
using Microsoft.Extensions.Logging;

namespace Common.Workers.Seller;

public abstract class AbstractSellerThread : ISellerWorker
{

    private readonly Random random;

    protected readonly SellerWorkerConfig config;

    protected int sellerId;

    private IDiscreteDistribution productIdGenerator;

    protected readonly ILogger logger;

    private Product[] products;

    // concurrent bag because of concurrent writes of different products
    protected readonly ConcurrentBag<TransactionIdentifier> submittedTransactions;
    protected readonly ConcurrentBag<TransactionOutput> finishedTransactions;
    protected readonly ConcurrentBag<TransactionMark> abortedTransactions;

    protected AbstractSellerThread(int sellerId, SellerWorkerConfig workerConfig, ILogger logger)
	{
        this.random = Random.Shared;
        this.logger = logger;
        this.submittedTransactions = new ConcurrentBag<TransactionIdentifier>();
        this.finishedTransactions = new ConcurrentBag<TransactionOutput>();
        this.abortedTransactions = new();
        this.sellerId = sellerId;
        this.config = workerConfig;
    }

    public void SetUp(List<Product> products, DistributionType keyDistribution)
    {
        this.products = products.ToArray();
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                 new DiscreteUniform(1, products.Count, Random.Shared) :
                                 new Zipf(WorkloadConfig.productZipfian, products.Count, Random.Shared);
        this.submittedTransactions.Clear();
        this.finishedTransactions.Clear();
        this.abortedTransactions.Clear();
    }

    /**
     * The method is only called if there are available products, so the while loop always finishes at some point
     */
    public void UpdatePrice(string tid)
    {
        int idx = this.productIdGenerator.Sample() - 1;
        object locked = products[idx];
        while(!Monitor.TryEnter(locked))
        {
            idx = this.productIdGenerator.Sample() - 1;
            locked = products[idx];
        }

        int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);
        var currPrice = products[idx].price;
        var newPrice = currPrice + ((currPrice * percToAdjust) / 100);

        try{
            SendUpdatePriceRequest(tid, products[idx], newPrice);
            // update price after successful request
            products[idx].price = newPrice;
        }
        finally
        {
            Monitor.Exit(locked);
        }
    }

    protected abstract void SendUpdatePriceRequest(string tid, Product productToUpdate, float newPrice);

    // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/statements/lock
    public void UpdateProduct(string tid)
    {
        int idx = this.productIdGenerator.Sample() - 1;
        object locked = products[idx];
        // only one update of a given version is allowed
        while(!Monitor.TryEnter(locked))
        {
            idx = this.productIdGenerator.Sample() - 1;
            locked = products[idx];
        }

        try
        {
            Product product = new Product(products[idx], tid);
            SendProductUpdateRequest(product, tid);
            // trick so customer do not need to synchronize to get a product (it may refer to an older version though)
            this.products[idx] = product;
        }
        finally
        {
            Monitor.Exit(locked);
        }
        
    }

    protected abstract void SendProductUpdateRequest(Product product, string tid);

    // yes, we may retrieve a product that is being concurrently deleted
    // at first, I was thinking to always get available product..
    // because concurrently a seller can delete a product and the time spent on finding a available product is lost
    public Product GetProduct(int idx)
    {
        return this.products[idx];
    }

    public abstract void BrowseDashboard(string tid);

    public List<TransactionOutput> GetFinishedTransactions()
    {
        var list = new List<TransactionOutput>();
        while (this.finishedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        var list = new List<TransactionIdentifier>();
        while (this.submittedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }

    public abstract void AddFinishedTransaction(TransactionOutput transactionOutput);

    public List<TransactionMark> GetAbortedTransactions()
    {
        var list = new List<TransactionMark>();
        while (this.abortedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }
}

