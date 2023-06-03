using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Http;
using Common.Entity;
using Common.Scenario.Seller;
using Common.Streaming;
using Common.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;

namespace Grains.Workers
{
	public class SellerWorker : Grain, ISellerWorker
    {
        private readonly Random random;

        private SellerConfiguration config;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> stream;

        private long sellerId;

        private NumberGenerator productIdGenerator;

        private readonly ILogger<SellerWorker> _logger;

        // to support: (i) customer product retrieval and (ii) the delete operation, since it uses a search string
        private List<Product> products;

        private readonly ISet<long> deletedProducts;

        public SellerWorker(ILogger<SellerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
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

        public Task Init(SellerConfiguration sellerConfig, List<Product> products)
        {

            this._logger.LogWarning("Init -> Seller worker {0} with #{1} product(s).", this.sellerId, products.Count);
            this.config = sellerConfig;
            this.products = products;

            this.products.Sort(productComparer);
            long lastId = this.products.ElementAt(this.products.Count-1).product_id;

            this._logger.LogWarning("Init -> Seller worker {0} first {1} last {2}.", this.sellerId, this.products.ElementAt(0).product_id, lastId);
             
            this.productIdGenerator = this.config.keyDistribution == Distribution.UNIFORM ?
                 new UniformLongGenerator(this.products[0].product_id, lastId) :
                 new ZipfianGenerator(this.products[0].product_id, lastId);

            return Task.CompletedTask;
        }

        public override async Task OnActivateAsync()
        {
            this.sellerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.stream = streamProvider.GetStream<Event>(StreamingConfiguration.SellerReactStreamId, this.sellerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ReactToLowStock);
                }
            }
            await this.stream.SubscribeAsync<Event>(ReactToLowStock);

            var workloadStream = streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, this.sellerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);
        }

        private async Task Run(int operation, StreamSequenceToken token)
        {
            if (this.config.delayBeforeStart > 0)
            {
                this._logger.LogWarning("Seller {0} delay before start: {1}", this.sellerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            }
            else
            {
                this._logger.LogWarning("Seller {0} NO delay before start!", this.sellerId);
            }

            if (operation < 1)
            {
                UpdatePrice();
                return;
            }
            DeleteProduct();
        }

        // driver will call
        // potentially could lookup by string
        private async void DeleteProduct()
        {
            if(this.deletedProducts.Count == products.Count)
            {
                this._logger.LogWarning("All products already deleted by seller {0}", this.sellerId);
                return;
            }
            
            long selectedProduct = this.productIdGenerator.NextValue();

            while (deletedProducts.Contains(selectedProduct))
            {
                selectedProduct = this.productIdGenerator.NextValue();
            }

            await Task.Run(async () =>
            {
                try
                {
                    var response = await HttpUtils.client.DeleteAsync(config.urls["products"] + "/" + selectedProduct);
                    response.EnsureSuccessStatusCode();
                    this.deletedProducts.Add(selectedProduct);
                    this._logger.LogWarning("Product {0} deleted by seller {0}", selectedProduct, this.sellerId);
                }
                catch (Exception) {
                    this._logger.LogError("Product {0} could not be deleted by seller {0}", selectedProduct, this.sellerId);
                }
            });
        }

        private async Task<List<Product>> GetOwnProducts()
        {
            HttpResponseMessage response = await Task.Run(async () =>
            {
                // [query string](https://en.wikipedia.org/wiki/Query_string)
                return await HttpUtils.client.GetAsync(config.urls["products"] + "?seller_id=" + this.sellerId);
            });
            // deserialize response
            response.EnsureSuccessStatusCode();
            string productsStr = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<Product>>(productsStr);
        }

        // driver will call
        private async void UpdatePrice()
        {
            // to simulate how a seller would interact with the platform
            this._logger.LogWarning("Seller {0} has started UpdatePrice", this.sellerId);

            // 1 - simulate seller browsing own main page (that will bring the product list)
            List<Product> products = await GetOwnProducts();

            int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
            await Task.Delay(delay);
            
            // 2 - from the products retrieved, select one or more and submit the updates
            // could attach a session id to the updates, so it is possible to track the transaction session afterwards
            int numberProductsToAdjustPrice = random.Next(1, products.Count+1);

            // select from distribution and put in a map
            var productsToUpdate = GetProductsToUpdate(products, numberProductsToAdjustPrice);

            // define perc to raise
            int percToAdjust = random.Next(config.adjustRange.Start.Value, config.adjustRange.End.Value);

            // 3 - update product objects
            foreach (var product in productsToUpdate)
            {
                var currPrice = product.price;
                product.price = currPrice + ( (currPrice * percToAdjust) / 100 );
            }

            // submit updates
            string productsUpdated = JsonConvert.SerializeObject(products.ToList());
            var task = await Task.Run(async () =>
            {
                // https://dev.olist.com/docs/editing-a-product
                HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Patch, config.urls["products"]);
                request.Content = HttpUtils.BuildPayload(productsUpdated);
                return await HttpUtils.client.SendAsync(request);
            });

            if (task.IsSuccessStatusCode)
            {
                this._logger.LogWarning("Seller {0} has finished price update.", this.sellerId);
                return;
            }
            this._logger.LogError("Seller {0} failed to update prices.", this.sellerId);
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

        private static readonly ProductEqualityComparer productEqualityComparer = new ProductEqualityComparer();

        private ISet<Product> GetProductsToUpdate(List<Product> products, int numberProducts)
        {
            ISet<Product> set = new HashSet<Product>(productEqualityComparer);

            var map = products.GroupBy(p => p.product_id).ToDictionary(group => group.Key, group => group.First());

            while(set.Count < numberProducts)
            {
                set.Add(map[this.productIdGenerator.NextValue()]);
            }
            return set;

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

