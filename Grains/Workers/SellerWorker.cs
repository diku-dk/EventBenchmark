using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Dynamic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Http;
using Common.Scenario.Entity;
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

        // has to make sure that generated product IDs are sequential for every seller
        private Range keyRange;

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
            this.config = sellerConfig;
            this.products = products;

            this.products.Sort(productComparer);
            long lastId = this.products[this.products.Count-1].product_id;

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
            await stream.SubscribeAsync<Event>(ReactToLowStock);

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
                _logger.LogInformation("Seller {0} delay before start: {1}", this.sellerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            }
            else
            {
                _logger.LogInformation("Seller {0} NO delay before start!", this.sellerId);
            }

            if (operation < 1)
            {
                UpdatePrice();
                return;
            }
            DeleteProduct();
        }

        // driver will call
        private void DeleteProduct()
        {
            if(deletedProducts.Count == products.Count) {
                _logger.LogWarning("All products already deleted for seller {0}", sellerId);
                return;
            }
            // potentially could lookup by string.
            long selectedProduct = this.productIdGenerator.NextValue();

            while (deletedProducts.Contains(selectedProduct)) {
                selectedProduct = this.productIdGenerator.NextValue();
            }
            deletedProducts.Add(selectedProduct);
            return;
        }

        private async Task<List<Product>> GetOwnProducts()
        {
            HttpResponseMessage response = await Task.Run(async () =>
            {
                // [query string](https://en.wikipedia.org/wiki/Query_string)
                return await HttpUtils.client.GetAsync(config.urls["products"] + "?seller_id=" + this.sellerId);
            });
            // deserialize response
            string productsStr = await response.Content.ReadAsStringAsync();
            return JsonConvert.DeserializeObject<List<Product>>(productsStr);
        }

        // driver will call
        private async void UpdatePrice()
        {
            // to simulate how a seller would interact with the platform
            _logger.LogInformation("Seller {0} has started UpdatePrice", this.sellerId);

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

            // 3 -
            foreach (var product in productsToUpdate)
            {
                var currPrice = product.price;
                product.price = currPrice + ( (currPrice * percToAdjust) / 100 );
            }

            // submit updates
            if (config.updateInBatch)
            {
                string productsUpdated = JsonConvert.SerializeObject(products.ToList());
                await Task.Run(() =>
                {
                    HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Put, config.urls["products"]);
                    request.Content = HttpUtils.BuildPayload(productsUpdated);
                    return HttpUtils.client.Send(request);
                });
            } else {
                foreach (var product in productsToUpdate)
                {
                    string prodUpdated = JsonConvert.SerializeObject(product);
                    await Task.Run(() =>
                    {
                        HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Put, config.urls["products"] + "/" + product.product_id);
                        request.Content = HttpUtils.BuildPayload(prodUpdated);
                        return HttpUtils.client.Send(request);
                        // return await HttpUtils.client.Send(config.urls["products"] + "/" + product.Id, HttpUtils.BuildPayload(prodUpdated));
                    });
                }
            }

            _logger.LogInformation("Seller {0} has finished price update.");

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
        // low stock, marketplace offer this estimation
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
            // can loop forever if all products are deleted
            /*
            while ( deletedProducts.Contains( selectedProduct )) {
                selectedProduct = this.productIdGenerator.NextValue();
            }
            */
            return Task.FromResult(selectedProduct);
        }

        //public Task<List<Product>> GetProducts()
        //{
        //    return Task.FromResult(this.products);
        //}
    }
}

