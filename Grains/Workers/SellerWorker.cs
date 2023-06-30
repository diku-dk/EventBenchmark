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
using Client.Streaming.Redis;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Threading;
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

        private SellerWorkerStatus status;

        private NumberGenerator productIdGenerator;

        private readonly ILogger<SellerWorker> _logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private IAsyncStream<SellerWorkerStatusUpdate> txStream;

        private readonly IDictionary<long,byte> deletedProducts;

        private readonly ConcurrentBag<TransactionIdentifier> submittedTransactions = new ConcurrentBag<TransactionIdentifier>();
        private readonly IList<TransactionOutput> finishedTransactions = new List<TransactionOutput>();

        private bool endToEndLatencyCollection = false;
        private CancellationTokenSource token = new CancellationTokenSource();
        private Task externalTask;
        private string channel;

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
            this.status = SellerWorkerStatus.IDLE;
            this.deletedProducts = new ConcurrentDictionary<long,byte>();
        }

        private sealed class ProductComparer : IComparer<Product>
        {
            public int Compare(Product x, Product y)
            {
                if (x.product_id < y.product_id) return 0; return 1;
            }
        }

        private static readonly ProductComparer productComparer = new ProductComparer();

        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products, bool endToEndLatencyCollection, string connection)
        {
            this._logger.LogWarning("Init -> Seller worker {0} with #{1} product(s).", this.sellerId, products.Count);
            this.config = sellerConfig;
            this.products = products;

            this.products.Sort(productComparer);
            int firstId = (int)this.products[0].product_id;
            int lastId = (int)this.products.ElementAt(this.products.Count - 1).product_id;

            this._logger.LogWarning("Init -> Seller worker {0} first {1} last {2}.", this.sellerId, this.products.ElementAt(0).product_id, lastId);

            this.productIdGenerator = this.config.keyDistribution == DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(((lastId - firstId) * 0.3) + firstId), firstId, lastId) :
                this.config.keyDistribution == DistributionType.UNIFORM ?
                 new UniformLongGenerator(firstId, lastId) :
                 new ZipfianGenerator(firstId, lastId);

            this.endToEndLatencyCollection = endToEndLatencyCollection;
            if (endToEndLatencyCollection) {
                this.channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(sellerId).ToString();
                this.externalTask = Task.Run(() => RedisUtils.Subscribe(connection, channel, token.Token, entry =>
                {
                    var now = DateTime.Now;
                    try
                    {
                        JObject d = JsonConvert.DeserializeObject<JObject>(entry.Values[0].Value.ToString());
                        TransactionMark mark = JsonConvert.DeserializeObject<TransactionMark>(d.SelectToken("['data']").ToString());
                        finishedTransactions.Add(new TransactionOutput(mark.tid, now));
                    }
                    catch (Exception) { }
                }));
            }

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

            this.txStream = streamProvider.GetStream<SellerWorkerStatusUpdate>(StreamingConstants.SellerStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        private async Task Run(TransactionInput txId, StreamSequenceToken token)
        {
            this.status = SellerWorkerStatus.RUNNING;
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
            this.status = SellerWorkerStatus.IDLE;
            // let emitter aware this request has finished
            _ = txStream.OnNextAsync(new SellerWorkerStatusUpdate(this.sellerId, this.status));
        }

        private async Task BrowseDashboard(int tid)
        {
            await Task.Run(() =>
            {
                try
                {
                    this._logger.LogWarning("Seller {0}: Dashboard will be queried...", this.sellerId);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
                    this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now));
                    var response = HttpUtils.client.Send(message);
                    response.EnsureSuccessStatusCode();
                    this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.Now));
                    this._logger.LogWarning("Seller {0}: Dashboard retrieved.", this.sellerId);
                }
                catch (Exception e)
                {
                    this._logger.LogError("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
                }
            });
        }

        private async Task DeleteProduct(int tid)
        {
            if(this.deletedProducts.Count() == this.products.Count())
            {
                this._logger.LogWarning("All products already deleted by seller {0}", this.sellerId);
                return;
            }
            
            long selectedProduct = this.productIdGenerator.NextValue();

            while (deletedProducts.ContainsKey(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }

            await Task.Run(() =>
            {
                try
                {
                    var obj = JsonConvert.SerializeObject(new DeleteProduct(sellerId, selectedProduct, tid));
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.productUrl);
                    message.Content = HttpUtils.BuildPayload(obj);
                    this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.DELETE_PRODUCT, DateTime.Now));
                    var response = HttpUtils.client.Send(message);
                    response.EnsureSuccessStatusCode();
                    // many threads can access the set at the same time...
                    this.deletedProducts.Add(selectedProduct, 0);
                    this._logger.LogWarning("Seller {0}: Product {1} deleted.", this.sellerId, selectedProduct);
                }
                catch (Exception e) {
                    this._logger.LogError("Seller {0}: Product {1} could not be deleted: {2}", this.sellerId, selectedProduct, e.Message);
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
            this._logger.LogWarning("Seller {0} has started UpdatePrice", this.sellerId);

            // 1 - simulate seller browsing own main page (that will bring the product list)
            var productsRetrieved = (await GetOwnProducts()).ToDictionary(k=>k.product_id,v=>v);

            int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
            await Task.Delay(delay);

            // 2 - select one to submit the update (based on distribution)
            long selectedProduct = this.productIdGenerator.NextValue();

            while (this.deletedProducts.ContainsKey(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }

            // define perc to raise
            int percToAdjust = random.Next(config.adjustRange.min, config.adjustRange.max);

            // 3 - get new price
            var currPrice = productsRetrieved[selectedProduct].price;
            var newPrice = currPrice + ( (currPrice * percToAdjust) / 100 );
            
            // 4 - submit update
            string serializedObject = JsonConvert.SerializeObject(new UpdatePrice(this.sellerId, selectedProduct, newPrice, tid));
            var resp = await Task.Run(() =>
            {
                // https://dev.olist.com/docs/editing-a-product
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.productUrl);
                request.Content = HttpUtils.BuildPayload(serializedObject);
                this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, DateTime.Now));
                return HttpUtils.client.Send(request);
            });

            if (resp.IsSuccessStatusCode)
            {
                this._logger.LogWarning("Seller {0}: Finished product {1} price update.", this.sellerId, selectedProduct);
                return;
            }
            this._logger.LogError("Seller {0} failed to update product {1} price.", this.sellerId, selectedProduct);
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

    }
}