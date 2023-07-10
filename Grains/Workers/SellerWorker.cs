using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Common.Http;
using Common.Entities;
using Common.Workload.Seller;
using Common.Streaming;
using Common.Distribution.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Requests;
using System.Collections.Concurrent;
using Orleans.Concurrency;

namespace Grains.Workers
{
    [Reentrant]
	public class SellerWorker : Grain, ISellerWorker
    {
        private readonly Random random;

        private SellerWorkerConfig config;

        private IStreamProvider streamProvider;

        private long sellerId;

        private NumberGenerator productIdGenerator;

        private readonly ILogger<SellerWorker> logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private IAsyncStream<int> txStream;

        private readonly IDictionary<long,byte> deletedProducts;

        private readonly IDictionary<long,TransactionIdentifier> submittedTransactions;
        private readonly IDictionary<long, TransactionOutput> finishedTransactions;

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this.logger = logger;
            this.random = new Random();
            this.deletedProducts = new ConcurrentDictionary<long,byte>();
            this.submittedTransactions = new ConcurrentDictionary<long, TransactionIdentifier>();
            this.finishedTransactions = new ConcurrentDictionary<long, TransactionOutput>();
        }

        private sealed class ProductComparer : IComparer<Product>
        {
            public int Compare(Product x, Product y)
            {
                if (x.product_id < y.product_id) return 0; return 1;
            }
        }

        private static readonly ProductComparer productComparer = new ProductComparer();

        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products)
        {
            this.logger.LogInformation("Init -> Seller worker {0} with #{1} product(s) [interactive mode: t/f]: {2}", this.sellerId, products.Count, sellerConfig.interactive);
            this.config = sellerConfig;
            this.products = products;

            this.products.Sort(productComparer);
            int firstId = (int)this.products[0].product_id;
            int lastId = (int)this.products.ElementAt(this.products.Count - 1).product_id;

            this.logger.LogInformation("Init -> Seller worker {0} first {1} last {2}.", this.sellerId, this.products.ElementAt(0).product_id, lastId);

            this.productIdGenerator = this.config.keyDistribution == DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(((lastId - firstId) * 0.3) + firstId), firstId, lastId) :
                this.config.keyDistribution == DistributionType.UNIFORM ?
                 new UniformLongGenerator(firstId, lastId) :
                 new ZipfianGenerator(firstId, lastId);

            this.submittedTransactions.Clear();
            this.finishedTransactions.Clear();
            this.deletedProducts.Clear();

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            var workloadStream = streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerStreamId, this.sellerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync(Run);

            this.txStream = streamProvider.GetStream<int>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        private async Task Run(TransactionInput txId, StreamSequenceToken token)
        {
            switch (txId.type)
            {
                case TransactionType.DASHBOARD:
                {
                    await BrowseDashboard(txId.tid);
                    break;
                }
                case TransactionType.DELETE_PRODUCT:
                {
                    await DeleteProduct(txId.tid);
                    break;
                }
                case TransactionType.PRICE_UPDATE:
                {
                    await UpdatePrice(txId.tid);
                    break;
                }
            }
        }

        private async Task BrowseDashboard(int tid)
        {
            await Task.Run(() =>
            {
                try
                {
                    this.logger.LogInformation("Seller {0}: Dashboard will be queried...", this.sellerId);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
                    this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now));
                    var response = HttpUtils.client.Send(message);
                    response.EnsureSuccessStatusCode();
                    this.finishedTransactions.Add(tid, new TransactionOutput(tid, DateTime.Now));
                    this.logger.LogInformation("Seller {0}: Dashboard retrieved.", this.sellerId);
                }
                catch (Exception e)
                {
                    this.logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
                }
                finally
                {
                    // let emitter aware this request has finished
                    _ = txStream.OnNextAsync(tid);
                }
            });
        }

        private async Task DeleteProduct(int tid)
        {
            this.logger.LogInformation("Seller {0}: Started delete product", this.sellerId);
            if (this.deletedProducts.Count() == this.products.Count())
            {
                this.logger.LogWarning("Seller {0}: All products already deleted by ", this.sellerId);
                // to ensure emitter send other transactions
                _ = txStream.OnNextAsync(tid);
                return;
            }
            
            long selectedProduct = this.productIdGenerator.NextValue();

            int count = 1;
            while (deletedProducts.ContainsKey(selectedProduct))
            {
                if(count == 10)
                {
                    this.logger.LogWarning("Seller {0}: Cannot define a product to delete! Cancelling this request!", this.sellerId);
                    _ = txStream.OnNextAsync(tid);
                    return;
                }
                selectedProduct = this.productIdGenerator.NextValue();
                count++;
            }

            await Task.Run(() =>
            {
                try
                {
                    this.logger.LogInformation("Seller {0}: Product {1} will be deleted...", this.sellerId, selectedProduct);
                    var obj = JsonConvert.SerializeObject(new DeleteProduct(this.sellerId, selectedProduct, tid));
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.productUrl);
                    message.Content = HttpUtils.BuildPayload(obj);
                    this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                    var response = HttpUtils.client.Send(message);
                    response.EnsureSuccessStatusCode();
                    this.deletedProducts.Add(selectedProduct, 0);
                    this.logger.LogInformation("Seller {0}: Product {1} deleted.", this.sellerId, selectedProduct);
                }
                catch (Exception e) {
                    this.logger.LogError("Seller {0}: Product {1} could not be deleted: {2}", this.sellerId, selectedProduct, e.Message);
                }
            });
        }

        private async Task<List<Product>> GetOwnProducts()
        {
            HttpResponseMessage response = await Task.Run(async () =>
            {
                // [query string](https://en.wikipedia.org/wiki/Query_string)
                return await HttpUtils.client.GetAsync(config.productUrl + "/" + this.sellerId);
            });
            // deserialize response
            response.EnsureSuccessStatusCode();
            string productsStr = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<Product>>(productsStr);
        }

        private async Task UpdatePrice(int tid)
        {
            // to simulate how a seller would interact with the platform
            this.logger.LogInformation("Seller {0}: Started price update", this.sellerId);

            if(this.deletedProducts.Count() == this.products.Count())
            {
                this.logger.LogWarning("No products to update since seller {0} has deleted every product already!", this.sellerId);
                // to ensure emitter send other transactions
                _ = txStream.OnNextAsync(tid);
                return;
            }

            if (config.interactive)
            {
                // 1 - simulate seller browsing own main page (that will bring the product list)
                var productsRetrieved = (await GetOwnProducts()).ToDictionary(k => k.product_id, v => v);

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                await Task.Delay(delay);
            }

            // 2 - select one to submit the update (based on distribution)
            long selectedProduct = this.productIdGenerator.NextValue();
            int count = 1;
            while (deletedProducts.ContainsKey(selectedProduct))
            {
                if (count == 10)
                {
                    this.logger.LogWarning("Seller {0}: Cannot define a product to delete! Cancelling this request!", this.sellerId);
                    _ = txStream.OnNextAsync(tid);
                    return;
                }
                selectedProduct = this.productIdGenerator.NextValue();
                count++;
            }


            // define perc to raise
            int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

            // 3 - get new price
            var currPrice = products.First(p=>p.product_id == selectedProduct).price;
            var newPrice = currPrice + ( (currPrice * percToAdjust) / 100 );
            
            // 4 - submit update
            var resp = await Task.Run(() =>
            {
                // https://dev.olist.com/docs/editing-a-product
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
                string serializedObject = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, selectedProduct, newPrice, tid));
                request.Content = HttpUtils.BuildPayload(serializedObject);
                this.submittedTransactions.Add(tid, new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.Now));
                return HttpUtils.client.Send(request);
            });

            if (resp.IsSuccessStatusCode)
            {
                this.logger.LogInformation("Seller {0}: Finished product {1} price update.", this.sellerId, selectedProduct);
                return;
            }
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, selectedProduct, resp.ReasonPhrase);
        }

        private sealed class ProductEqualityComparer : IEqualityComparer<Product>
        {
            public bool Equals(Product x, Product y)
            {
                return (x.product_id == y.product_id);
            }

            public int GetHashCode([DisallowNull] Product obj)
            {
                return obj.product_id.GetHashCode();
            }
        }

        public Task<long> GetProductId()
        {
            long selectedProduct = this.productIdGenerator.NextValue();
            return Task.FromResult(selectedProduct);
        }

        public Task<Product> GetProduct()
        {
            long productId = this.productIdGenerator.NextValue();
            return Task.FromResult( products.First(p => p.product_id == productId) );
        }

        public Task<List<Latency>> Collect(DateTime startTime)
        {
            var targetValues = submittedTransactions.Values.Where(e => e.timestamp.CompareTo(startTime) >= 0);
            var latencyList = new List<Latency>(submittedTransactions.Count());
            foreach (var entry in targetValues)
            {
                if (finishedTransactions.ContainsKey(entry.tid))
                {
                    var res = finishedTransactions[entry.tid];
                    latencyList.Add(new Latency(entry.tid, entry.type,
                        (res.timestamp - entry.timestamp).TotalMilliseconds ));
                }
            }
            return Task.FromResult(latencyList);
        }

        public Task RegisterFinishedTransaction(TransactionOutput output)
        {
            finishedTransactions.Add(output.tid, output);
            return Task.CompletedTask;
        }
    }
}