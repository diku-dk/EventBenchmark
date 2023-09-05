using Common.Distribution;
using Common.Entities;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using MathNet.Numerics.Distributions;
using Microsoft.Extensions.Logging;

namespace Common.Workers;

public abstract class AbstractSellerThread : ISellerWorker
{

    private readonly Random random;

    protected readonly SellerWorkerConfig config;

    protected int sellerId;

    private IDiscreteDistribution productIdGenerator;

    protected readonly HttpClient httpClient;

    protected readonly ILogger logger;

    private Product[] products;

    protected readonly List<TransactionIdentifier> submittedTransactions;

    protected readonly List<TransactionOutput> finishedTransactions;

    protected AbstractSellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger)
	{
        this.random = Random.Shared;
        this.logger = logger;
        this.submittedTransactions = new List<TransactionIdentifier>();
        this.finishedTransactions = new List<TransactionOutput>();
        this.sellerId = sellerId;
        this.httpClient = httpClient;
        this.config = workerConfig;
    }

    public void SetUp(List<Product> products, DistributionType keyDistribution)
    {
        this.products = products.ToArray();
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                 new DiscreteUniform(1, products.Count, Random.Shared) :
                                 new Zipf(0.99, products.Count, Random.Shared);
    }

    /**
     * The method is only called if there are available products, so the while loop always finishes at some point
     */
    public void UpdatePrice(int tid)
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

    protected abstract void SendUpdatePriceRequest(int tid, Product productToUpdate, float newPrice);

    // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/statements/lock
    public void UpdateProduct(int tid)
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

    protected abstract void SendProductUpdateRequest(Product product, int tid);

    // yes, we may retrieve a product that is being concurrently deleted
    // at first, I was thinking to always get available product..
    // because concurrently a seller can delete a product and the time spent on finding a available product is lost
    public Product GetProduct(int idx)
    {
        return this.products[idx];
    }

    public void BrowseDashboard(int tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, DateTime.UtcNow));
            var response = httpClient.Send(message);
            if (response.IsSuccessStatusCode)
            {
                this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
            }
            else
            {
                this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, response.ReasonPhrase);
            }
        }
        catch (Exception e)
        {
            this.logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
        }
    }

    public List<TransactionOutput> GetFinishedTransactions()
    {
        return this.finishedTransactions;
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.submittedTransactions;
    }

}

