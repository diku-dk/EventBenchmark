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
using Common.Requests;

namespace Grains.Workers
{
	public class SellerWorker : Grain, ISellerWorker
    {
        private readonly Random random;

        private SellerWorkerConfig config;

        private IStreamProvider streamProvider;

        //private IAsyncStream<Event> stream;

        private long sellerId;

        private SellerWorkerStatus status;

        private NumberGenerator productIdGenerator;

        private readonly ILogger<SellerWorker> _logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private IAsyncStream<SellerWorkerStatusUpdate> txStream;

        private readonly ISet<long> deletedProducts;

        private readonly IList<TransactionIdentifier> submittedTransactions = new List<TransactionIdentifier>();
        private readonly IList<TransactionOutput> finishedTransactions = new List<TransactionOutput>();

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
            this.status = SellerWorkerStatus.IDLE;
            this.deletedProducts = new HashSet<long>();
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
            this._logger.LogWarning("Init -> Seller worker {0} with #{1} product(s).", this.sellerId, products.Count);
            this.config = sellerConfig;
            this.products = products;

            this.products.Sort(productComparer);
            int firstId = (int) this.products[0].product_id;
            int lastId = (int) this.products.ElementAt(this.products.Count-1).product_id;

            this._logger.LogWarning("Init -> Seller worker {0} first {1} last {2}.", this.sellerId, this.products.ElementAt(0).product_id, lastId);

            this.productIdGenerator = this.config.keyDistribution == DistributionType.NON_UNIFORM ? new NonUniformDistribution((int)(((lastId - firstId) * 0.3) + firstId), firstId, lastId) :
                this.config.keyDistribution == DistributionType.UNIFORM ?
                 new UniformLongGenerator(firstId, lastId) :
                 new ZipfianGenerator(firstId, lastId);

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            /*
            this.stream = streamProvider.GetStream<Event>(StreamingConstants.SellerReactStreamId, this.sellerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToLowStock);
                }
            }
            await this.stream.SubscribeAsync<Event>(ReactToLowStock);
            */

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
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, config.urls["sellers"] + "/" + this.sellerId);
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

            while (deletedProducts.Contains(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }

            await Task.Run(() =>
            {
                try
                {
                    var obj = JsonConvert.SerializeObject(new DeleteProduct(sellerId, selectedProduct, tid));
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Delete, config.urls["products"]);
                    message.Content = HttpUtils.BuildPayload(obj);
                    var response = HttpUtils.client.Send(message);
                    response.EnsureSuccessStatusCode();
                    this.deletedProducts.Add(selectedProduct);
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
                return await HttpUtils.client.GetAsync(config.urls["products"] + "/" + this.sellerId);
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

            while (this.deletedProducts.Contains(selectedProduct))
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
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.urls["products"]);
                request.Content = HttpUtils.BuildPayload(serializedObject);
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

        // app will send
        // low stock, marketplace offers this estimation
        // received from a continuous query (basically implement a model)[defined function or sql]
        // the standard deviation for the continuous query
        private Task ReactToLowStock(Event lowStockWarning, StreamSequenceToken token)
        {
            // given a distribution, the seller can increase and send an event increasing the stock or not
            // maybe it is reasonable to always increase stock to a minimum level
            // let's do it first
            return Task.CompletedTask;
        }

        public Task<long> GetProductId()
        {
            long selectedProduct = this.productIdGenerator.NextValue();
            return Task.FromResult(selectedProduct);
        }

    }
}