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
    private SellerWorkerConfig config;

    private int sellerId;

    private IDiscreteDistribution fixedIdGenerator;
    private IDiscreteDistribution dynamicIdGenerator;

    private readonly HttpClient httpClient;

    private readonly ILogger logger;

    // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
    private List<Product> products;
    private List<Product> availableProducts;
    
    private readonly List<TransactionIdentifier> submittedTransactions;
    private readonly List<TransactionOutput> finishedTransactions;

    private static object LockDist = new object();
    private static object LockAvail = new object();

    public SellerThread BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig, List<Product> products)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);
        var httpClient = httpClientFactory.CreateClient();
        return new SellerThread(sellerId, httpClient, workerConfig, products, logger);
    }

    private SellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig sellerConfig, List<Product> products, ILogger logger)
	{
        this.random = new Random();
        this.logger = logger;
        this.submittedTransactions = new List<TransactionIdentifier>();
        this.finishedTransactions = new List<TransactionOutput>();
        this.sellerId = sellerId;
        this.httpClient = httpClient;
        this.config = sellerConfig;
        this.products = products;
        this.availableProducts = new(products);
        this.fixedIdGenerator = this.config.keyDistribution == DistributionType.UNIFORM ?
                new DiscreteUniform(1, products.Count, new Random()) :
                new Zipf(0.99, products.Count, new Random());
        this.dynamicIdGenerator =
            this.config.keyDistribution == DistributionType.UNIFORM ?
                new DiscreteUniform(1, products.Count, new Random()) :
                new Zipf(0.99, products.Count, new Random()); 
    }

    public void Run(int tid, TransactionType type)
    {
        switch (type)
        {
            case TransactionType.DASHBOARD:
            {
                BrowseDashboard(tid);
                break;
            }
            case TransactionType.DELETE_PRODUCT:
            {
                DeleteProduct(tid);
                break;
            }
            case TransactionType.PRICE_UPDATE:
            {   
                UpdatePrice(tid);
                break;
            }
        }
    }

    private void UpdatePrice(int tid)
    {

        int idx = this.dynamicIdGenerator.Sample() - 1;

        int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

        var productToUpdate = availableProducts[idx];
        var currPrice = productToUpdate.price;
        var newPrice = currPrice + ((currPrice * percToAdjust) / 100);
        productToUpdate.price = newPrice;

        HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
        string serializedObject = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, productToUpdate.product_id, newPrice, tid));
        request.Content = HttpUtils.BuildPayload(serializedObject);

        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
            this.logger.LogDebug("Seller {0}: Finished product {1} price update.", this.sellerId, productToUpdate.product_id);
        }
        else
        {
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
        }
    }

    private void BrowseDashboard(int tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.UtcNow));
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

    private void UpdateDist(int upper)
    {
        this.dynamicIdGenerator =
            this.config.keyDistribution == DistributionType.UNIFORM ?
                new DiscreteUniform(1, upper, new Random()) :
                new Zipf(0.99, upper, new Random());
    }

    private void DeleteProduct(int tid)
    {
        int idx = this.dynamicIdGenerator.Sample() - 1;
       
        Product toDelete;
        int count = 0;
        lock (LockAvail)
        {
            toDelete = availableProducts[idx];
            availableProducts.RemoveAt(idx);
            count = availableProducts.Count();
        }
        UpdateDist(count);
        
        var obj = JsonConvert.SerializeObject(new DeleteProduct(this.sellerId, toDelete.product_id, tid));
        HttpRequestMessage message = new(HttpMethod.Delete, config.productUrl)
        {
            Content = HttpUtils.BuildPayload(obj)
        };

        var now = DateTime.UtcNow;
        var resp = httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, now));
        }
    }

    public bool HasAvailableProducts()
    {
        bool res = false;
        lock (LockAvail)
        {
            res = this.availableProducts.Count() > 0;
        }
        return res;
    }

    // synchronization only across customers
    public Product GetProduct()
	{
        int idx;
        lock (LockDist)
        {
            idx = this.fixedIdGenerator.Sample() - 1;
        }
        return products[idx];
    }

}


