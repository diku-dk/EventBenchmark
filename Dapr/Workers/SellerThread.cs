using Common.Distribution;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;

namespace Daprr.Workers;

public sealed class SellerThread
{
    private readonly Random random;
    private readonly SellerWorkerConfig config;

    private int sellerId;

    private IDiscreteDistribution productIdGenerator;

    private readonly HttpClient httpClient;

    private readonly ILogger logger;

    private Product[] products;

    private readonly List<TransactionIdentifier> submittedTransactions;
    private readonly List<TransactionOutput> finishedTransactions;

    public static SellerThread BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);
        var httpClient = httpClientFactory.CreateClient();
        return new SellerThread(sellerId, httpClient, workerConfig, logger);
    }

    private SellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger)
	{
        this.random = new Random();
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
                                 new DiscreteUniform(1, products.Count, new Random()) :
                                 new Zipf(0.99, products.Count, new Random());
    }

    public void Run(int tid, TransactionType type)
    {
        switch (type)
        {
            case TransactionType.QUERY_DASHBOARD:
            {
                BrowseDashboard(tid);
                break;
            }
            case TransactionType.UPDATE_PRODUCT:
            {
                UpdateProduct(tid);
                break;
            }
            case TransactionType.PRICE_UPDATE:
            {   
                UpdatePrice(tid);
                break;
            }
        }
    }

    /**
     * The method is only called if there are available products, so the while loop always finishes at some point
     */
    private void UpdatePrice(int tid)
    {
        int idx = this.productIdGenerator.Sample() - 1;
        var productToUpdate = products[idx];

        int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);
        var currPrice = productToUpdate.price;
        var newPrice = currPrice + ((currPrice * percToAdjust) / 100);
        productToUpdate.price = newPrice;

        HttpRequestMessage request = new(HttpMethod.Patch, config.productUrl);
        string serializedObject = JsonConvert.SerializeObject(new PriceUpdate(this.sellerId, productToUpdate.product_id, newPrice, tid));
        request.Content = HttpUtils.BuildPayload(serializedObject);

        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
        }
        else
        {
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
        }
    }

    // https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/statements/lock
    private void UpdateProduct(int tid)
    {
        int idx = this.productIdGenerator.Sample() - 1;

        // only one update of a given version is allowed
        while(!Monitor.TryEnter(products[idx]))
        {
            idx = this.productIdGenerator.Sample() - 1;
        }

        try
        {
            Product product = new Product(products[idx], tid);
            SendProductUpdateRequest(product, tid);
            // trick so customer do not need to synchronize to get a product (it may refer to an older version though)
            products[idx] = product;
        }
        finally
        {
            Monitor.Exit(products[idx]);
        }
        
    }

    private void SendProductUpdateRequest(Product product, int tid)
    {
        var obj = JsonConvert.SerializeObject(product);
        HttpRequestMessage message = new(HttpMethod.Put, config.productUrl)
        {
            Content = HttpUtils.BuildPayload(obj)
        };

        var now = DateTime.UtcNow;
        httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
        this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
    }

    // yes, we may retrieve a product that is being concurrently deleted
    // at first, I was thinking to always get available product..
    // because concurrently a seller can delete a product and the time spent on finding a available product is lost
    public Product GetProduct(int idx)
    {
        return this.products[idx];
    }

    private void BrowseDashboard(int tid)
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

