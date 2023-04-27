using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Http;
using Common.Scenario.Customer;
using Common.Entity;
using Common.State;
using Common.Streaming;
using Common.YCSB;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;

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

        private CustomerConfiguration config;

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

            // to notify transaction orchestrator about status update
            this.txStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfiguration.CustomerStreamId, StreamingConfiguration.TransactionStreamNameSpace);

        }

        public CustomerWorker(ILogger<CustomerWorker> logger)
        {
            this._logger = logger;
            this.random = new Random();
            this.status = CustomerWorkerStatus.IDLE;
        }

        public async Task Init(CustomerConfiguration config)
        {
            this._logger.LogWarning("Customer worker {0} Init", this.customerId);
            this.config = config;
            this.sellerIdGenerator = this.config.sellerDistribution == Distribution.UNIFORM ?
                new UniformLongGenerator(this.config.sellerRange.Start.Value, this.config.sellerRange.End.Value) :
                new ZipfianGenerator(this.config.sellerRange.Start.Value, this.config.sellerRange.End.Value);
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

            if (this.config.delayBeforeStart > 0)
            {
                this._logger.LogWarning("Customer {0} delay before start: {1}", this.customerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            } else
            {
                this._logger.LogWarning("Customer {0} NO delay before start!", this.customerId);
            }

            this.status = CustomerWorkerStatus.BROWSING;

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            this._logger.LogWarning("Customer {0} has this number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            var keyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);

            this._logger.LogWarning("Customer {0} will start browsing", this.customerId);

            // browsing
            Browse(keyMap);

            this._logger.LogWarning("Customer " + customerId + " finished browsing!");

            // TODO should we also model this behavior?
            int numberOfKeysToCheckout =
                random.Next(1, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);

            // adding to cart
            var task = new TaskCompletionSource();
            AddToCart(DefineKeysToCheckout(keyMap.ToList(), numberOfKeysToCheckout), task);

            await task.Task;
            // taskcompletion to avoid the subsequent lines to continue
            if (task.Task.IsCanceled)
            {
                InformFailedCheckout();
                return; // no need to continue
            }

            // get cart
            GetCart();

            // checkout
            if(await Checkout())
            {
                
                this.status = CustomerWorkerStatus.CHECKOUT_SENT;
                this._logger.LogWarning("Customer {0} sent the checkout successfully", customerId);

                // the timer is disposed if the grain is deactivated
                // but unlikely to be deactivated after 1000
                // to be sure, we would need a background scheduler. an external task could also do the job...
                this.timer = RegisterTimer(UpdateStatusAsync, null, TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(2000));
         
            } else
            {
                // fail and deciding not to send treated in the same way
                InformFailedCheckout();
            }

            return;
        }

        // pull-based. wait for cart to be open again
        private async Task UpdateStatusAsync(object arg)
        {
            this._logger.LogWarning("Timer fired for customer {0}", this.customerId);
            // only send the message if the cart actor status is open, otherwise it will receive exception
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, this.cartUrl + "/" + this.customerId);
            var response = HttpUtils.client.Send(message);
            var stream = response.Content.ReadAsStream();
            StreamReader reader = new StreamReader(stream);
            string productRet = reader.ReadToEnd();
            CartState state = JsonConvert.DeserializeObject<CartState>(productRet);
            if (state.status == Status.OPEN)
            {
                this.status = CustomerWorkerStatus.IDLE;
                this._logger.LogWarning("Timer in customer {0} reporting status update: {1}", this.customerId, this.status);
                await txStream.OnNextAsync(new CustomerStatusUpdate(this.customerId, this.status));
                this.timer.Dispose();
            }
        }  

        private IDisposable timer;

        private async void InformFailedCheckout()
        {
            this.status = CustomerWorkerStatus.CHECKOUT_FAILED;

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, cartUrl);
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
            if (this.config.checkoutDistribution[random.Next(0, this.config.checkoutDistribution.Length)] == 0)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_NOT_SENT;
                this._logger.LogWarning("Customer {0} decided to not send a checkout.", this.customerId);
                return false;
            }

            this._logger.LogWarning("Customer {0} decided to send a checkout.", this.customerId);

            // inform checkout intent. optional feature

            HttpResponseMessage resp = await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/" + this.customerId + "/checkout");
                message.Content = BuildCheckoutPayload(this.customer);
                return HttpUtils.client.Send(message);
            });


            return resp != null && resp.IsSuccessStatusCode;

        }

        private async void AddToCart(ISet<long> keyMap, TaskCompletionSource task)
        {
            HttpResponseMessage response;
            foreach (var productId in keyMap)
            {
                this._logger.LogWarning("Customer {0} adding product {1} to cart", this.customerId, productId);
                await Task.Run(() =>
                {
                    try
                    {
                        HttpRequestMessage message1 = new HttpRequestMessage(HttpMethod.Get, productUrl + "/" + productId);
                        response = HttpUtils.client.Send(message1);

                        // add to cart
                        if (response.Content.Headers.ContentLength == 0)
                        {
                            this._logger.LogWarning("Response content for product {0} is empty! {1}", productId);
                            return new HttpResponseMessage(HttpStatusCode.InternalServerError);
                        }

                        var qty = random.Next(this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value);

                        // var productRet = await response.Content.ReadAsStringAsync();
                        var stream = response.Content.ReadAsStream();
                      
                        StreamReader reader = new StreamReader(stream);
                        string productRet = reader.ReadToEnd();
                        var payload = BuildCartItem(productRet, qty);

                        HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Put, cartUrl + "/" + customerId);
                        message2.Content = payload;

                        response = HttpUtils.client.Send(message2);

                        return response;
                    }
                    catch (Exception e)
                    {
                        this._logger.LogWarning("Exception Message: {0} Customer {1} Url {2} Key {3}", e.Message, customerId, productUrl, productId);
                        task.SetCanceled();
                        if (e is HttpRequestException)
                        {
                            HttpRequestException e_ = (HttpRequestException)e;
                            return new HttpResponseMessage((HttpStatusCode)e_.StatusCode);
                        }
                        return new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
                    }

                });

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                // artificial delay after adding the product
                await Task.Delay(delay);

            }

            task.SetResult();
        }

        private async void Browse(ISet<long> keyMap)
        {
            int delay;
            foreach (var productId in keyMap)
            {
                this._logger.LogWarning("Customer {0} browsing product {1}", this.customerId, productId);
                delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
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
                case "out-of-stock": { await this.ReactToOutOfStock(data.payload); break; }
                case "price-update": { await this.ReactToPriceUpdate(data.payload); break; }
                case "product-unavailable": { await this.ReactToProductUnavailable(data.payload); break; }
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

        private static StringContent BuildCartItem(string productPayload, int quantity)
        {
            Product product = JsonConvert.DeserializeObject<Product>(productPayload);
            // build a basket item
            BasketItem basketItem = new()
            {
                ProductId = product.id,
                SellerId = product.seller_id,
                // ProductName = product.name,
                UnitPrice = product.price,
                Quantity = quantity,
                FreightValue = 0.0m // not modeling freight value in this version
            };
            var payload = JsonConvert.SerializeObject(basketItem);
            return HttpUtils.BuildPayload(payload);
        }

        private StringContent BuildCheckoutPayload(Customer customer)
        {

            // TODO define payment type from distribution
            // TODO define installments from distribution
            // TODO define voucher from distribution

            // build
            CustomerCheckout basketCheckout = new()
            {
                CustomerId = customer.id,
                FirstName = customer.first_name,
                LastName = customer.last_name,
                City = customer.city,
                Street = customer.address,
                Complement = customer.complement,
                State = customer.state,
                ZipCode = customer.zip_code_prefix,
                PaymentType = PaymentType.CREDIT_CARD.ToString(),
                Installments = random.Next(1,11),
                CardNumber = customer.card_number,
                CardHolderName = customer.card_holder_name,
                CardExpiration = customer.card_expiration,
                CardSecurityNumber = customer.card_security_number,
                CardBrand = customer.card_type
            };
            var payload = JsonConvert.SerializeObject(basketCheckout);
            return HttpUtils.BuildPayload(payload);
        }

    }
}