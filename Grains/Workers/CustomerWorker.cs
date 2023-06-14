using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Http;
using Common.Workload.Customer;
using Common.Entities;
using Common.Streaming;
using Common.Distribution.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;
using Common.Event;
using Common.Distribution;

namespace Grains.Workers
{

    /**
     * Driver-side, client-side code, which is also run in Orleans silo
     * Nice example of tasks in a customer session:
     * https://github.com/GoogleCloudPlatform/microservices-demo/blob/main/src/loadgenerator/locustfile.py
     */
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {
        private readonly Random random;

        private CustomerWorkerConfig config;

        private NumberGenerator sellerIdGenerator;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> stream;

        private IAsyncStream<CustomerStatusUpdate> txStream;

        private long customerId;

        private CustomerWorkerStatus status;

        private string productUrl;

        private string cartUrl;

        private Customer customer;

        private readonly ILogger<CustomerWorker> _logger;

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfig.DefaultStreamProvider);
            this.stream = streamProvider.GetStream<Event>(StreamingConfig.CustomerReactStreamId, this.customerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ProcessEventAsync);
                }
            }
            await stream.SubscribeAsync<Event>(ProcessEventAsync);

            var workloadStream = streamProvider.GetStream<int>(StreamingConfig.CustomerStreamId, this.customerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);

            // to notify transaction orchestrator about status update
            this.txStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfig.CustomerStreamId, StreamingConfig.TransactionStreamNameSpace);

        }

        public CustomerWorker(ILogger<CustomerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
            this.status = CustomerWorkerStatus.IDLE;
        }

        public async Task Init(CustomerWorkerConfig config)
        {
            this._logger.LogWarning("Customer worker {0} Init", this.customerId);
            this.config = config;
            this.sellerIdGenerator = this.config.sellerDistribution == DistributionType.UNIFORM ?
                new UniformLongGenerator(this.config.sellerRange.min, this.config.sellerRange.max) :
                new ZipfianGenerator(this.config.sellerRange.min, this.config.sellerRange.max);
            this.productUrl = this.config.urls["products"];
            this.cartUrl = this.config.urls["carts"];
            this.customer = await GetCustomer(this.config.urls["customers"], customerId);
        }

        private async Task<Customer> GetCustomer(string customerUrl, long customerId)
        {
            this._logger.LogWarning("Customer worker {0} retrieving customer object...", this.customerId);
            HttpResponseMessage resp = await Task.Run(async () =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, customerUrl + "/" + customerId);
                return await HttpUtils.client.SendAsync(message);
            });
            var str = await resp.Content.ReadAsStringAsync();
            this._logger.LogWarning("Customer worker {0} retrieved customer object {1}", this.customerId, str);
            return JsonConvert.DeserializeObject<Customer>(str);
        }

        /**
         * From the list of browsed keys, picks randomly the keys to checkout
         */
        private ISet<long> DefineKeysToCheckout(List<long> browsedKeys, int numberOfKeysToCheckout)
        {
            ISet<long> set = new HashSet<long>(numberOfKeysToCheckout);
            while (set.Count < numberOfKeysToCheckout)
            {
                set.Add(browsedKeys[random.Next(0, browsedKeys.Count)]);
            }
            return set;
        }

        private async Task<ISet<long>> DefineKeysToBrowseAsync(int numberOfKeysToBrowse)
        {
            ISet<long> keyMap = new HashSet<long>(numberOfKeysToBrowse);
            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            long grainId;
            long productId;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                grainId = this.sellerIdGenerator.NextValue();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(grainId);

                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
                productId = await sellerWorker.GetProductId();
                while (keyMap.Contains(productId))
                {
                    grainId = this.sellerIdGenerator.NextValue();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(grainId);
                    productId = await sellerWorker.GetProductId();
                }

                keyMap.Add(productId);
                sb.Append(productId);
                if (i < numberOfKeysToBrowse - 1) sb.Append(", ");
            }
            this._logger.LogWarning("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            return keyMap;
        }

        private async Task Run(int obj, StreamSequenceToken token)
        {
            this._logger.LogWarning("Customer {0} started!", this.customerId);

            if(this.customer == null)
            {
                this._logger.LogError("Customer object is not set in Customer Worker {0}. Cancelling browsing.", this.customerId);
                return;
            }

            this.status = CustomerWorkerStatus.BROWSING;

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            this._logger.LogWarning("Customer {0} has the following number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            var keyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);

            this._logger.LogWarning("Customer {0} will start browsing", this.customerId);

            // browsing
            await Browse(keyMap);

            this._logger.LogWarning("Customer " + customerId + " finished browsing!");

            // TODO should we also model this behavior?
            int numberOfKeysToCheckout =
                random.Next(1, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);

            // adding to cart
            try
            {
                await AddToCart(DefineKeysToCheckout(keyMap.ToList(), numberOfKeysToCheckout));
            }
            catch (Exception e)
            {
                this._logger.LogError(e.Message);
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                await InformFailedCheckout();
                return; // no need to continue  
            }

            // get cart
            GetCart();

            // checkout
            if(await Checkout())
            {
                this.status = CustomerWorkerStatus.CHECKOUT_SENT;
                this._logger.LogWarning("Customer {0} sent the checkout successfully", customerId);
                await UpdateStatusAsync();
            } else
            {
                // fail and deciding not to send treated in the same way
                await InformFailedCheckout();
            }

            return;
        }

        private async Task UpdateStatusAsync()
        {
            this.status = CustomerWorkerStatus.IDLE;
            await txStream.OnNextAsync(new CustomerStatusUpdate(this.customerId, this.status)); 
        }

        private async Task InformFailedCheckout()
        {
            // just cleaning cart state for next browsing
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, cartUrl + "/" + customerId + "/seal");
            HttpUtils.client.Send(message);

            await txStream.OnNextAsync(new CustomerStatusUpdate(this.customerId, this.status));
            this._logger.LogWarning("Customer {0} checkout failed", this.customerId);
        }

        /**
         * Simulating the customer browsing the cart before checkout
         * could verify whether the cart contains all the products chosen, otherwise throw exception
         */
        private async void GetCart()
        {
            await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, cartUrl + "/" + this.customerId);
                return HttpUtils.client.Send(message);
            });
        }

        private async Task<bool> Checkout()
        {
            // define whether client should send a checkout request
            if (random.Next(0, 100) > this.config.checkoutProbability)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_NOT_SENT;
                this._logger.LogWarning("Customer {0} decided to not send a checkout.", this.customerId);
                return false;
            }

            this._logger.LogWarning("Customer {0} decided to send a checkout.", this.customerId);

            // inform checkout intent. optional feature
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/" + this.customerId + "/checkout");
            message.Content = BuildCheckoutPayload(this.customer);
            HttpResponseMessage resp = await Task.Run(() =>
            {
                return HttpUtils.client.Send(message);
            });

            if (resp == null)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                return false;
            }
            if (resp.IsSuccessStatusCode) return true;

            // perhaps price divergence
            // apply new prices and send again
            if(resp.StatusCode == HttpStatusCode.MethodNotAllowed)
            {
                resp = await Task.Run(() =>
                {
                    return HttpUtils.client.Send(message);
                });
            }

            if (resp == null || !resp.IsSuccessStatusCode)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                return false;
            }

            // very unlikely to have another price update (considering the distribution is low)
            return true;
        }

        private async Task AddToCart(ISet<long> keyMap)
        {
            HttpResponseMessage response;
            //List<Task> tasks = new();
            foreach (var productId in keyMap)
            {
                this._logger.LogWarning("Customer {0} start adding product {1} to cart", this.customerId, productId);
                await Task.Run(() =>
                {
                    try
                    {
                        HttpRequestMessage message1 = new HttpRequestMessage(HttpMethod.Get, productUrl + "/" + productId);
                        response = HttpUtils.client.Send(message1);

                        // add to cart
                        if (response.Content.Headers.ContentLength == 0)
                        {
                            this._logger.LogWarning("Response content for product {0} is empty!", productId);
                            return response;
                        }

                        this._logger.LogWarning("Customer {0} product {1} retrieved", this.customerId, productId);

                        var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);

                        // var productRet = await response.Content.ReadAsStringAsync();
                        var stream = response.Content.ReadAsStream();

                        StreamReader reader = new StreamReader(stream);
                        string productRet = reader.ReadToEnd();
                        var payload = BuildCartItem(productRet, qty);

                        HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Patch, cartUrl + "/" + customerId + "/add");
                        message2.Content = payload;

                        this._logger.LogWarning("Customer {0} sending product {1} payload to cart", this.customerId, productId);
                        response = HttpUtils.client.Send(message2);

                        return response;
                    }
                    catch (Exception e)
                    {
                        this._logger.LogWarning("Exception Message: {0} Customer {1} Url {2} Key {3}", e.Message, customerId, productUrl, productId);
                        if (e is HttpRequestException)
                        {
                            HttpRequestException e_ = (HttpRequestException)e;
                            return new HttpResponseMessage((HttpStatusCode)e_.StatusCode);
                        }
                        return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                    }

                });

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                // artificial delay after adding the product
                await Task.Delay(delay);

            }

            // await Task.WhenAll(tasks);

        }

        private async Task Browse(ISet<long> keyMap)
        {
            int delay;
            foreach (var productId in keyMap)
            {
                this._logger.LogWarning("Customer {0} browsing product {1}", this.customerId, productId);
                delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                await Task.Run(async () =>
                {
                    try
                    {
                        await HttpUtils.client.GetAsync(productUrl + "/" + productId); 
                    }
                    catch (Exception e)
                    {
                        this._logger.LogWarning("Exception Message: {0} Customer {1} Url {2} Key {3}", e.Message, customerId, productUrl, productId);
                    }

                });
                // artificial delay after retrieving the product
                await Task.Delay(delay);
            }
        }

        private async Task ProcessEventAsync(Event data, StreamSequenceToken token)
        {
            // for each event, forward to the respective handler
            switch (data.topic)
            {
                case "abandoned-cart": { await this.ReactToAbandonedCart(data.payload); break; }
                case "payment-rejected": { await this.ReactToPaymentRejected(data.payload); break; }
                    // TODO merge the 3 below...
                case "out-of-stock": { await this.ReactToOutOfStock(data.payload); break; }
                //case "price-update": { await this.ReactToPriceUpdate(data.payload); break; }
                //case "product-unavailable": { await this.ReactToProductUnavailable(data.payload); break; }
                default: { _logger.LogWarning("Topic: " + data.topic + " has no associated reaction in customer grain " + customerId); break; }
            }
            return;
        }

        // AFTER CHECKOUT

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

        // ON CHECKOUT

        private Task ReactToPriceUpdate(string priceUpdateEvent)
        {
            // submit some operations.. get request (customer url) and then a post (rating)
            return Task.CompletedTask;
        }

        private Task ReactToProductUnavailable(string priceUpdateEvent)
        {
            // 
            return Task.CompletedTask;
        }

        private StringContent BuildCartItem(string productPayload, int quantity)
        {

            Product product = JsonConvert.DeserializeObject<Product>(productPayload);

            // define voucher from distribution
            var vouchers = Array.Empty<decimal>();
            int probVoucher = this.random.Next(0, 101);
            if(probVoucher <= this.config.voucherProbability)
            {
                int numVouchers = this.random.Next(1, this.config.maxNumberVouchers + 1);
                vouchers = new decimal[numVouchers];
                for(int i = 0; i < numVouchers; i++)
                {
                    vouchers[i] = this.random.Next(1, 10);
                }
            }
            
            // build a basket item
            CartItem basketItem = new CartItem(
                    product.seller_id,
                    product.product_id,
                    product.name,
                    product.price,
                    0.0m, // not modeling freight value in this version
                    quantity,
                    vouchers
            );
            var payload = JsonConvert.SerializeObject(basketItem);
            return HttpUtils.BuildPayload(payload);
        }

        private StringContent BuildCheckoutPayload(Customer customer)
        {

            // define payment type randomly
            var typeIdx = random.Next(1, 4);
            PaymentType type = typeIdx > 2 ? PaymentType.CREDIT_CARD : typeIdx > 1 ? PaymentType.DEBIT_CARD : PaymentType.BOLETO;

            // build
            CustomerCheckout basketCheckout = new CustomerCheckout(
                customer.id,
                customer.first_name,
                customer.last_name,
                customer.city,
                customer.address,
                customer.complement,
                customer.state,
                customer.zip_code,
                type.ToString(),
                customer.card_number,
                customer.card_holder_name,
                customer.card_expiration,
                customer.card_security_number,
                customer.card_type,
                random.Next(1, 11) // installments
            );

            var payload = JsonConvert.SerializeObject(basketCheckout);
            return HttpUtils.BuildPayload(payload);
        }

    }
}