using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Http;
using Common.Scenario.Customer;
using Common.Scenario.Entity;
using Common.Streaming;
using Common.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;

namespace Grains.Workers
{
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {
        private readonly Random random;

        private CustomerConfiguration config;

        private NumberGenerator sellerIdGenerator;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> stream;

        private IAsyncStream<CustomerStatusUpdate> txStream;

        private long customerId;

        private CustomerStatus status;

        private string productUrl;

        private string cartUrl;

        private readonly ILogger<CustomerWorker> _logger;

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.stream = streamProvider.GetStream<Event>(StreamingConfiguration.CustomerReactStreamId, this.customerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ProcessEventAsync);
                }
            }
            await stream.SubscribeAsync<Event>(ProcessEventAsync);

            var workloadStream = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, this.customerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);

            this.txStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfiguration.CustomerStreamId, StreamingConfiguration.TransactionStreamNameSpace);

        }

        public CustomerWorker(ILogger<CustomerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
        }

        public Task Init(CustomerConfiguration config)
        {
            this.config = config;
            this.sellerIdGenerator = this.config.sellerDistribution == Distribution.UNIFORM ?
                new UniformLongGenerator(this.config.sellerRange.Start.Value, this.config.sellerRange.End.Value) :
                new ZipfianGenerator(this.config.sellerRange.Start.Value, this.config.sellerRange.End.Value);
            this.productUrl = this.config.urls["products"];
            this.cartUrl = this.config.urls["carts"];
            return Task.CompletedTask;
        }

        private async Task<Dictionary<long, int>> DefineKeysToBrowseAsync(int numberOfKeysToBrowse)
        {
            // this dct must be numberOfKeysToCheckout
            Dictionary<long, int> keyToQtyMap = new Dictionary<long, int>(numberOfKeysToBrowse);
            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            long grainId;
            long productId;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                grainId = this.sellerIdGenerator.NextValue();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(grainId);
                // pick products from seller distribution
                productId = await sellerWorker.GetProductId();
                while (keyToQtyMap.ContainsKey(productId))
                {
                    grainId = this.sellerIdGenerator.NextValue();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(grainId);
                    productId = await sellerWorker.GetProductId();
                }

                keyToQtyMap.Add(productId, 0);
                sb.Append(productId);
                if (i < numberOfKeysToBrowse - 1) sb.Append(", ");
            }
            _logger.LogInformation("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            return keyToQtyMap;
        }

        private async Task Run(int obj, StreamSequenceToken token)
        {
            _logger.LogInformation("Customer {0} started!", this.customerId);

            if (this.config.delayBeforeStart > 0)
            {
                _logger.LogInformation("Customer {0} delay before start: {1}", this.customerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            }
            else
            {
                _logger.LogInformation("Customer {0} NO delay before start!", this.customerId);
            }

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            _logger.LogInformation("Customer {0} has this number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            var keyToQtyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);

            HttpResponseMessage[] responses = new HttpResponseMessage[numberOfKeysToBrowse];

            // should we also model this behavior?
            int numberOfKeysToCheckout =
                random.Next(0, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart));

            _logger.LogInformation("Customer {0} will start browsing", this.customerId);

            // browsing
            int idx = 0;
            foreach (KeyValuePair<long, int> entry in keyToQtyMap)
            {
                responses[idx] = await Task.Run(async () =>
                {

                    _logger.LogInformation("Customer {0} Task {1}", this.customerId, idx+1);

                    int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                    HttpResponseMessage response;
                    try
                    {
                        response = await HttpUtils.client.GetAsync(productUrl + "/" + entry.Key);

                        // artificial delay after retrieving the product
                        await Task.Delay(delay);

                        if (numberOfKeysToCheckout > 0)
                        {

                            // add to cart
                            if (response.Content.Headers.ContentLength == 0)
                            {
                                _logger.LogInformation("Response content for product {0} is empty! {1}", entry.Key);
                                return new HttpResponseMessage(HttpStatusCode.InternalServerError);
                            }

                            // mark this product as already added to cart
                            keyToQtyMap[entry.Key] = random.Next(this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value);

                            var productRet = await response.Content.ReadAsStringAsync();
                            string payload = BuildCartItemPayloadFunc(productRet, keyToQtyMap[entry.Key]);

                            response = await HttpUtils.client.PostAsync(cartUrl + "/" + customerId + "/add", HttpUtils.BuildPayload(payload));
                            numberOfKeysToCheckout--;

                        }
                        return response;
                    }
                    catch (Exception e)
                    {
                        _logger.LogInformation("Exception Message: {0} Customer {1} Url {2} Key {3}", e.Message, customerId, productUrl, entry.Key);
                        if(e is HttpRequestException) {
                            HttpRequestException e_ = (HttpRequestException)e;
                            return new HttpResponseMessage((HttpStatusCode)e_.StatusCode);
                        }
                        return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                    }

                });

                // check for errors
                if (!responses[idx].IsSuccessStatusCode)
                {
                    _logger.LogInformation("Customer " + customerId + " could not finish browsing due to errors!");
                    return;
                }

                idx++;
            }

            _logger.LogInformation("Customer " + customerId + " finished browsing!");

            // define whether client should send a checkout request
            if (this.config.checkoutDistribution[random.Next(0, this.config.checkoutDistribution.Length)] == 1)
            {
                _logger.LogInformation("Customer " + customerId + " decided to send a checkout!");
                await Task.Run(() =>
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/" + customerId + "/checkout");
                    HttpUtils.client.Send(message);
                });
                this.status = CustomerStatus.CHECKOUT_SENT;
            }
            else
            {
                this.status = CustomerStatus.CHECKOUT_NOT_SENT;
                _logger.LogInformation("Customer " + customerId + " decided not to send a checkout!");
            }

            await txStream.OnNextAsync(new CustomerStatusUpdate(this.customerId, this.status));

            return;
        }

        private async Task ProcessEventAsync(Event data, StreamSequenceToken token)
        {
            // for each event, forward to the respective handler
            switch (data.topic)
            {
                case "abandoned-cart": { await this.ReactToAbandonedCart(data.payload); break; }
                case "payment-rejected": { await this.ReactToPaymentRejected(data.payload); break; }
                case "out-of-stock": { await this.ReactToOutOfStock(data.payload); break; }
                case "price-update": { await this.ReactToPriceUpdate(data.payload); break; }
                default: { _logger.LogInformation("Topic: " + data.topic + " has no associated reaction in customer grain " + customerId); break; }
            }
            return;
        }

        private Task ReactToAbandonedCart(string abandonedCartEvent)
        {
            // TODO given a probability, can checkout or not
            return Task.CompletedTask;
        }

        private Task ReactToPaymentRejected(string paymentDeniedEvent)
        {
            // random. insert a new payment type or cancel the order
            return Task.CompletedTask;
        }

        private Task ReactToOutOfStock(string outOfStockEvent)
        {
            return Task.CompletedTask;
        }

        private Task ReactToPriceUpdate(string priceUpdateEvent)
        {
            // submit some operations.. get request (customer url) and and then a post (rating)
            return Task.CompletedTask;
        }

        // is this customer-based?
        private static string BuildCartItemPayloadFunc(string productPayload, int quantity)
        {
            /*
            // parse into json, add quantity attribute, envelop again
            var obj = JsonConvert.DeserializeObject<ExpandoObject>(productPayload) as IDictionary<string, Object>;
            obj["quantity"] = quantity;
            return JsonConvert.SerializeObject(obj);
            */
            Product product = JsonConvert.DeserializeObject<Product>(productPayload);
            // TODO build a basket item
            return null;
        }

    }
}