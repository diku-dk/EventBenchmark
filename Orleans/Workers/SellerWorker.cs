//using Common.Http;
//using Common.Entities;
//using Common.Workload.Seller;
//using Common.Streaming;
//using Microsoft.Extensions.Logging;
//using Newtonsoft.Json;
//using Orleans.Streams;
//using Common.Distribution;
//using Common.Workload;
//using Common.Workload.Metrics;
//using Common.Requests;
//using Orleans.Concurrency;
//using Grains.WorkerInterfaces;
//using MathNet.Numerics.Distributions;

//namespace Grains.Workers;

//[Reentrant]
//public class SellerGrain : Grain, ISellerGrain
//{
//    private readonly Random random;

//    private SellerWorkerConfig config;

//    private IStreamProvider streamProvider;

//    private int sellerId;

//    private IDiscreteDistribution productIdGenerator;

//    private readonly HttpClient httpClient;

//    private readonly ILogger<SellerGrain> logger;

//    // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
//    private List<Product> products;

//    private readonly IDictionary<int, byte> deletedProducts;

//    private readonly List<TransactionIdentifier> submittedTransactions;
//    private readonly List<TransactionOutput> finishedTransactions;

//    public SellerGrain(HttpClient httpClient, ILogger<SellerGrain> logger)
//    {
//        this.httpClient = httpClient;
//        this.logger = logger;
//        this.random = new Random();
//        this.deletedProducts = new Dictionary<int, byte>();
//        this.submittedTransactions = new List<TransactionIdentifier>();
//        this.finishedTransactions = new List<TransactionOutput>();
//    }

//    public Task Init(SellerWorkerConfig sellerConfig, List<Product> products)
//    {
//        this.logger.LogDebug("Seller worker {0}: Init with #{1} product(s) [interactive mode:]: {2}", this.sellerId, products.Count, sellerConfig.interactive);
//        this.config = sellerConfig;
//        this.products = products;
//        this.productIdGenerator =
//            this.config.keyDistribution == DistributionType.UNIFORM ?
//             new DiscreteUniform(1, products.Count, new Random()) :
//             new Zipf(0.99, products.Count, new Random());

//        this.submittedTransactions.Clear();
//        this.finishedTransactions.Clear();
//        this.deletedProducts.Clear();

//        return Task.CompletedTask;
//    }

//    public override async Task OnActivateAsync(CancellationToken cancellationToken)
//    {
//        this.sellerId = (int)this.GetPrimaryKeyLong();
//        this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
//        var workloadStream = streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, this.sellerId.ToString());
//        var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
//        if (subscriptionHandles_.Count > 0)
//        {
//            foreach (var subscriptionHandle in subscriptionHandles_)
//            {
//                await subscriptionHandle.ResumeAsync(Run);
//            }
//        }
//        await workloadStream.SubscribeAsync(Run);
//    }

//    private async Task Run(TransactionInput txId, StreamSequenceToken token)
//    {
//        switch (txId.type)
//        {
//            case TransactionType.QUERY_DASHBOARD:
//                {
//                    BrowseDashboard(txId.tid);
//                    break;
//                }
//            case TransactionType.UPDATE_PRODUCT:
//                {
//                    DeleteProduct(txId.tid);
//                    break;
//                }
//            case TransactionType.PRICE_UPDATE:
//                {
//                    if (config.interactive)
//                    {
//                        // 1 - simulate seller browsing own main page (that will bring the product list)
//                        await GetOwnProducts();

//                        int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
//                        await Task.Delay(delay);
//                    }
//                    UpdatePrice(txId.tid);
//                    break;
//                }
//        }
//    }

//    private void BrowseDashboard(int tid)
//    {
//        try
//        {
//            this.logger.LogDebug("Seller {0}: Dashboard will be queried...", this.sellerId);
//            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
//            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, DateTime.UtcNow));
//            var response = httpClient.Send(message);
//            if (response.IsSuccessStatusCode)
//            {
//                this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
//                this.logger.LogDebug("Seller {0}: Dashboard retrieved.", this.sellerId);
//            }
//            else
//            {
//                this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, response.ReasonPhrase);
//            }
//        }
//        catch (Exception e)
//        {
//            this.logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
//        }
//    }

//    private void DeleteProduct(int tid)
//    {
//        this.logger.LogDebug("Seller {0}: Started delete product", this.sellerId);
//        if (this.deletedProducts.Count() == this.products.Count())
//        {
//            this.logger.LogWarning("No products to delete since seller {0} has deleted every product already!", this.sellerId);
//            // FIXME must add to result queue of emitter... this might be causing degradation...
//            return;
//        }

//        int idx = this.productIdGenerator.Sample() - 1;

//        int count = 1;
//        while (deletedProducts.ContainsKey(idx))
//        {
//            if (count > 5)
//            {
//                this.logger.LogWarning("Seller {0}: Cannot define a product to delete! Cancelling this request at {0}", this.sellerId, DateTime.UtcNow);
//                return;
//            }
//            idx = this.productIdGenerator.Sample() - 1;
//            count++;
//        }
//        this.deletedProducts.Add(idx, 0);

//        this.logger.LogDebug("Seller {0}: Product {1} will be deleted.", this.sellerId, products[idx].product_id);
//        var obj = JsonConvert.SerializeObject(new UpdateProduct(this.sellerId, products[idx].product_id, tid));
//        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.productUrl);
//        message.Content = HttpUtils.BuildPayload(obj);

//        var now = DateTime.UtcNow;
//        var resp = httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
//        if (resp.IsSuccessStatusCode)
//        {
//            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
//            this.logger.LogDebug("Seller {0}: Product {1} deleted.", this.sellerId, products[idx].product_id);
//        }
//        else
//        {
//            this.logger.LogError("Seller {0}: Product {1} could not be deleted: {2}", this.sellerId, products[idx].product_id, resp.ReasonPhrase);
//            this.deletedProducts.Remove(idx);
//        }

//    }

//    private async Task<List<Product>> GetOwnProducts()
//    {
//        // [query string](https://en.wikipedia.org/wiki/Query_string)
//        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.productUrl + "/" + this.sellerId);
//        HttpResponseMessage response = httpClient.Send(message);

//        // deserialize response
//        if (response.IsSuccessStatusCode)
//        {
//            string productsStr = await response.Content.ReadAsStringAsync();
//            return JsonConvert.DeserializeObject<List<Product>>(productsStr);
//        }
//        else
//        {
//            return new();
//        }
//    }

//    private void UpdatePrice(int tid)
//    {
//        // to simulate how a seller would interact with the platform
//        this.logger.LogDebug("Seller {0}: Started price update", this.sellerId);

//        if (this.deletedProducts.Count() == this.products.Count())
//        {
//            this.logger.LogWarning("No products to update since seller {0} has deleted every product already!", this.sellerId);
//            return;
//        }

//        // 2 - select one to submit the update (based on distribution)
//        int idx = this.productIdGenerator.Sample() - 1;
//        int count = 1;
//        while (deletedProducts.ContainsKey(idx))
//        {
//            if (count == 10)
//            {
//                this.logger.LogWarning("Seller {0}: Cannot define a product to update! Cancelling this request!", this.sellerId);
//                return;
//            }
//            idx = this.productIdGenerator.Sample() - 1;
//            count++;
//        }

//        // define perc to raise
//        int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

//        // 3 - get new price
//        var productToUpdate = products[idx];
//        var currPrice = productToUpdate.price;
//        var newPrice = currPrice + ((currPrice * percToAdjust) / 100);
//        productToUpdate.price = newPrice;

//        // 4 - submit update
//        // https://dev.olist.com/docs/editing-a-product
//        HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
//        string serializedObject = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, productToUpdate.product_id, newPrice, tid));
//        request.Content = HttpUtils.BuildPayload(serializedObject);

//        var initTime = DateTime.UtcNow;
//        var resp = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
//        if (resp.IsSuccessStatusCode)
//        {
//            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
//            this.logger.LogDebug("Seller {0}: Finished product {1} price update.", this.sellerId, productToUpdate.product_id);
//        }
//        else
//        {
//            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
//        }
//    }

//    public Task<int> GetProductId()
//    {
//        var idx = this.productIdGenerator.Sample() - 1;
//        return Task.FromResult(products[idx].product_id);
//    }

//    public Task<Product> GetProduct()
//    {
//        var idx = this.productIdGenerator.Sample() - 1;
//        return Task.FromResult(products[idx]);
//    }

//    public Task<(List<TransactionIdentifier>, List<TransactionOutput>)> Collect()
//    {
//        return Task.FromResult((submittedTransactions, finishedTransactions));
//    }

//}