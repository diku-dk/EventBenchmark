using Common.Http;
using Common.Entities;
using Common.Workload.Seller;
using Common.Streaming;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Streams;
using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Requests;
using System.Collections.Concurrent;
using Orleans.Concurrency;
using Grains.WorkerInterfaces;
using MathNet.Numerics.Distributions;

namespace Grains.Workers
{
    [Reentrant]
	public class SellerWorker : Grain, ISellerWorker
    {
        private readonly Random random;

        private SellerWorkerConfig config;

        private IStreamProvider streamProvider;

        private long sellerId;

        private IDiscreteDistribution productIdGenerator;

        private readonly ILogger<SellerWorker> logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private readonly IDictionary<long,byte> deletedProducts;

        private readonly IDictionary<long,TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this.logger = logger;
            this.random = new Random();
            this.deletedProducts = new Dictionary<long,byte>();
            this.submittedTransactions = new Dictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new Dictionary<long, TransactionOutput>();
        }

        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products)
        {
            this.logger.LogDebug("Seller worker {0}: Init with #{1} product(s) [interactive mode:]: {2}", this.sellerId, products.Count, sellerConfig.interactive);
            this.config = sellerConfig;
            this.products = products;
            this.productIdGenerator = 
                this.config.keyDistribution == DistributionType.UNIFORM ?
                 new DiscreteUniform(1, products.Count, new Random()) :
                 new Zipf(0.95, products.Count, new Random());

            this.submittedTransactions.Clear();
            this.finishedTransactions.Clear();
            this.deletedProducts.Clear();

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            var workloadStream = streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, this.sellerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync(Run);
        }

        private async Task Run(TransactionInput txId, StreamSequenceToken token)
        {
            switch (txId.type)
            {
                case TransactionType.DASHBOARD:
                {
                    BrowseDashboard(txId.tid);
                    break;
                }
                case TransactionType.DELETE_PRODUCT:
                {
                    DeleteProduct(txId.tid);
                    break;
                }
                case TransactionType.PRICE_UPDATE:
                {
                    if (config.interactive)
                    {
                        // 1 - simulate seller browsing own main page (that will bring the product list)
                        await GetOwnProducts();

                        int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                        await Task.Delay(delay);
                    }
                    UpdatePrice(txId.tid);
                    break;
                }
            }
        }

        private void BrowseDashboard(int tid)
        {
            try
            {
                this.logger.LogDebug("Seller {0}: Dashboard will be queried...", this.sellerId);
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
                this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.UtcNow));
                //var response = await HttpUtils.client.SendAsync(message);
                var response = HttpUtils.client.Send(message);
                if (response.IsSuccessStatusCode)
                {
                    this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.UtcNow));
                    this.logger.LogDebug("Seller {0}: Dashboard retrieved.", this.sellerId);
                } else
                {
                    this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, response.ReasonPhrase);
                }
            }
            catch (Exception e)
            {
                this.logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
            } 
        }

        private void DeleteProduct(int tid)
        {
            this.logger.LogDebug("Seller {0}: Started delete product", this.sellerId);
            if (this.deletedProducts.Count() == this.products.Count())
            {
                this.logger.LogWarning("No products to delete since seller {0} has deleted every product already!", this.sellerId);
                return;
            }
            
            int idx = this.productIdGenerator.Sample() - 1;

            int count = 1;
            while (deletedProducts.ContainsKey(idx) )
            {
                if(count == 10)
                {
                    this.logger.LogWarning("Seller {0}: Cannot define a product to delete! Cancelling this request!", this.sellerId);
                    return;
                }
                idx = this.productIdGenerator.Sample() - 1;
                count++;
            }
            this.deletedProducts.Add(idx, 0);

            this.logger.LogDebug("Seller {0}: Product {1} will be deleted.", this.sellerId, products[idx].product_id);
            var obj = JsonConvert.SerializeObject(new DeleteProduct(this.sellerId, products[idx].product_id, tid));
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.productUrl);
            message.Content = HttpUtils.BuildPayload(obj);

            this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.UtcNow));
            //var resp = await HttpUtils.client.SendAsync(message, HttpCompletionOption.ResponseHeadersRead);
            var resp = HttpUtils.client.Send(message, HttpCompletionOption.ResponseHeadersRead);

            if (resp.IsSuccessStatusCode)
            {
                this.logger.LogDebug("Seller {0}: Product {1} deleted.", this.sellerId, products[idx].product_id);
            }
            else
            {
                this.logger.LogError("Seller {0}: Product {1} could not be deleted: {2}", this.sellerId, products[idx].product_id, resp.ReasonPhrase);
                this.deletedProducts.Remove(idx);
            }

        }

        private async Task<List<Product>> GetOwnProducts()
        {
            // [query string](https://en.wikipedia.org/wiki/Query_string)
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.productUrl + "/" + this.sellerId);
            HttpResponseMessage response = HttpUtils.client.Send( message );
          
            // deserialize response
            if (response.IsSuccessStatusCode)
            {
                string productsStr = await response.Content.ReadAsStringAsync();
                return JsonConvert.DeserializeObject<List<Product>>(productsStr);
            }else
            {
                return new();
            }
        }

        private void UpdatePrice(int tid)
        {
            // to simulate how a seller would interact with the platform
            this.logger.LogDebug("Seller {0}: Started price update", this.sellerId);

            if(this.deletedProducts.Count() == this.products.Count())
            {
                this.logger.LogWarning("No products to update since seller {0} has deleted every product already!", this.sellerId);
                return;
            }

            // 2 - select one to submit the update (based on distribution)
            int idx = this.productIdGenerator.Sample() - 1;
            int count = 1;
            while (deletedProducts.ContainsKey(idx))
            {
                if (count == 10)
                {
                    this.logger.LogWarning("Seller {0}: Cannot define a product to update! Cancelling this request!", this.sellerId);
                    return;
                }
                idx = this.productIdGenerator.Sample() - 1;
                count++;
            }

            // define perc to raise
            int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

            // 3 - get new price
            var productToUpdate = products[idx];
            var currPrice = productToUpdate.price;
            var newPrice = currPrice + ( (currPrice * percToAdjust) / 100 );
            productToUpdate.price = newPrice;
            
            // 4 - submit update
            // https://dev.olist.com/docs/editing-a-product
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
            string serializedObject = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, productToUpdate.product_id, newPrice, tid));
            request.Content = HttpUtils.BuildPayload(serializedObject);

            this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.UtcNow));
            // var resp = await HttpUtils.client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
            var resp = HttpUtils.client.Send(request, HttpCompletionOption.ResponseHeadersRead);

            if (resp.IsSuccessStatusCode)
            {
                this.logger.LogDebug("Seller {0}: Finished product {1} price update.", this.sellerId, productToUpdate.product_id);
                return;
            }
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);

        }

        public Task<long> GetProductId()
        {
            var idx = this.productIdGenerator.Sample() - 1;
            return Task.FromResult(products[idx].product_id);
        }

        public Task<Product> GetProduct()
        {
            var idx = this.productIdGenerator.Sample() - 1;
            return Task.FromResult(products[idx]);
        }

        public Task<List<Latency>> Collect(DateTime finishTime)
        {
            var targetValues = finishedTransactions.Values.Where(e => e.timestamp.CompareTo(finishTime) <= 0);
            var latencyList = new List<Latency>(targetValues.Count());
            foreach (var entry in targetValues)
            {
                if (!submittedTransactions.ContainsKey(entry.tid))
                {
                    logger.LogWarning("Cannot find correspondent submitted TID from finished transaction {0}", entry);
                    continue;
                }
                var init = submittedTransactions[entry.tid];
                latencyList.Add(new Latency(entry.tid, init.type,
                    (entry.timestamp - init.timestamp).TotalMilliseconds, entry.timestamp));
            }
            return Task.FromResult(latencyList);
        }

        public Task RegisterFinishedTransaction(TransactionOutput output)
        {
            if (!finishedTransactions.TryAdd(output.tid, output))
            {
                logger.LogWarning("[{0}] The specified TID has already been added to the finished set at {1}",
                    output.timestamp, finishedTransactions[output.tid]);
            }
            // finishedTransactions.Add(output.tid, output);
            return Task.CompletedTask;
        }
    }
}