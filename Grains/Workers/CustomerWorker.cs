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
using Common.Distribution;
using Common.Requests;
using Common.Workload;
using Common.Workload.Metrics;
using Client.Streaming.Redis;
using System.Threading;
using Newtonsoft.Json.Linq;

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

        private IAsyncStream<CustomerWorkerStatusUpdate> txStream;

        private long customerId;

        private Customer customer;

        private CustomerWorkerStatus status;

        private readonly IList<TransactionIdentifier> submittedTransactions = new List<TransactionIdentifier>();
        private readonly IList<TransactionOutput> finishedTransactions = new List<TransactionOutput>();

        private readonly ILogger<CustomerWorker> logger;

        private CancellationTokenSource token = new CancellationTokenSource();
        private Task externalTask;
        private string channel;

        public override async Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.CustomerStreamId, this.customerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync(Run);

            // to notify transaction orchestrator about status update
            this.txStream = streamProvider.GetStream<CustomerWorkerStatusUpdate>(StreamingConstants.CustomerStreamId, StreamingConstants.TransactionStreamNameSpace);
        }

        public CustomerWorker(ILogger<CustomerWorker> logger)
        {
            this.logger = logger;
            this.random = new Random();
            this.status = CustomerWorkerStatus.IDLE;
        }

        public Task Init(CustomerWorkerConfig config, Customer customer, bool endToEndLatencyCollection, string connection = "")
        {
            this.logger.LogWarning("Customer worker {0} Init", this.customerId);
            this.config = config;
            this.customer = customer;
            this.sellerIdGenerator = this.config.sellerDistribution == DistributionType.NON_UNIFORM ?
                new NonUniformDistribution( (int)(this.config.sellerRange.max * 0.3), this.config.sellerRange.min, this.config.sellerRange.max) :
                this.config.sellerDistribution == DistributionType.UNIFORM ?
                new UniformLongGenerator(this.config.sellerRange.min, this.config.sellerRange.max) :
                new ZipfianGenerator(this.config.sellerRange.min, this.config.sellerRange.max);

            if (endToEndLatencyCollection)
            {
                this.channel = new StringBuilder(TransactionType.CUSTOMER_SESSION.ToString()).Append('_').Append(customerId).ToString();
                this.externalTask = Task.Run(() => RedisUtils.Subscribe(connection, channel, token.Token, entry =>
                {
                    // This code runs on the thread pool scheduler, not on Orleans task scheduler
                    var now = DateTime.Now;
                    logger.LogInformation("Customer worker {0}: Mark received from channel {0}: {1}", this.customerId, channel, entry.ToString());

                    // parse redis value so we can know which transaction has finished
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

        /**
         * From the list of browsed keys, picks randomly the keys to checkout
         */
        private ISet<(long sellerId, long productId)> DefineKeysToCheckout(List<(long sellerId, long productId)> browsedKeys, int numberOfKeysToCheckout)
        {
            ISet<(long sellerId, long productId)> set = new HashSet<(long sellerId, long productId)>(numberOfKeysToCheckout);
            while (set.Count < numberOfKeysToCheckout)
            {
                set.Add(browsedKeys[random.Next(0, browsedKeys.Count)]);
            }
            return set;
        }

        private async Task<ISet<(long sellerId,long productId)>> DefineKeysToBrowseAsync(int numberOfKeysToBrowse)
        {
            ISet<(long sellerId, long productId)> keyMap = new HashSet<(long sellerId, long productId)>(numberOfKeysToBrowse);
            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            long sellerId;
            long productId;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                sellerId = this.sellerIdGenerator.NextValue();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);

                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
                productId = await sellerWorker.GetProductId();
                while (keyMap.Contains((sellerId,productId)))
                {
                    sellerId = this.sellerIdGenerator.NextValue();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);
                    productId = await sellerWorker.GetProductId();
                }

                keyMap.Add((sellerId,productId));
                sb.Append(sellerId).Append("-").Append(productId);
                if (i < numberOfKeysToBrowse - 1) sb.Append(" | ");
            }
            this.logger.LogWarning("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            return keyMap;
        }

        private async Task Run(int tid, StreamSequenceToken token)
        {
            this.logger.LogWarning("Customer worker {0} processing new task...", this.customerId);

            this.status = CustomerWorkerStatus.BROWSING;

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            this.logger.LogWarning("Customer {0} number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            var keyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);

            this.logger.LogWarning("Customer {0} started browsing...", this.customerId);

            // browsing
            await Browse(keyMap);

            this.logger.LogWarning("Customer worker {0} finished browsing.", this.customerId);

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
                this.logger.LogError(e.Message);
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                await InformFailedCheckout(tid);
                return; // no need to continue  
            }

            await GetCart();

            await Checkout(tid);
        }

        private async Task UpdateStatusAsync(int tid)
        {
            this.status = CustomerWorkerStatus.IDLE;
            await txStream.OnNextAsync(new CustomerWorkerStatusUpdate(this.customerId, this.status)); 
        }

        private async Task InformFailedCheckout(int tid)
        {
            // just cleaning cart state for next browsing
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/seal");
            HttpUtils.client.Send(message);

            await txStream.OnNextAsync(new CustomerWorkerStatusUpdate(this.customerId, this.status));
            this.logger.LogWarning("Customer {0} checkout failed", this.customerId);
        }

        private async Task InformSucceededCheckout(int tid)
        {
            this.status = CustomerWorkerStatus.CHECKOUT_SENT;
            this.logger.LogWarning("Customer {0} sent the checkout successfully", customerId);
            await UpdateStatusAsync(tid);
        }

        /**
         * Simulating the customer browsing the cart before checkout
         * could verify whether the cart contains all the products chosen, otherwise throw exception
         */
        private async Task GetCart()
        {
            await Task.Run(() =>
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, this.config.cartUrl + "/" + this.customerId);
                return HttpUtils.client.Send(message);
            });
        }

        private async Task Checkout(int tid)
        {
            // define whether client should send a checkout request
            if (random.Next(0, 100) > this.config.checkoutProbability)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_NOT_SENT;
                this.logger.LogWarning("Customer {0} decided to not send a checkout.", this.customerId);
                await InformFailedCheckout(tid);
                return;
            }

            this.logger.LogWarning("Customer {0} decided to send a checkout.", this.customerId);

            // inform checkout intent. optional feature
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, this.config.cartUrl + "/" + this.customerId + "/checkout");
            message.Content = BuildCheckoutPayload(tid, this.customer);
            TransactionIdentifier txId = null;
            HttpResponseMessage resp = await Task.Run(() =>
            {
                txId = new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now);
                return HttpUtils.client.Send(message);
            });

            if (resp == null)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                await InformFailedCheckout(tid);
                return;
            }
            if (resp.IsSuccessStatusCode)
            {
                submittedTransactions.Add(txId);
                await InformSucceededCheckout(tid);
                return;
            }

            // perhaps there is price divergence. checking out again means the customer agrees with the new prices
            if(resp.StatusCode == HttpStatusCode.MethodNotAllowed)
            {
                resp = await Task.Run(() =>
                {
                    txId = new TransactionIdentifier(tid, TransactionType.DASHBOARD, DateTime.Now);
                    return HttpUtils.client.Send(message);
                });
            }

            if (resp == null || !resp.IsSuccessStatusCode)
            {
                this.status = CustomerWorkerStatus.CHECKOUT_FAILED;
                await InformFailedCheckout(tid);
                return;
            }

            submittedTransactions.Add(txId);
            // very unlikely to have another price update (considering the distribution is low)
            await InformSucceededCheckout(tid);
            
        }

        private async Task AddToCart(ISet<(long sellerId, long productId)> keyMap)
        {
            foreach (var entry in keyMap)
            {
                this.logger.LogWarning("Customer {0}: Adding seller {1} product {2} to cart", this.customerId, entry.sellerId, entry.productId);
                await Task.Run(() =>
                {
                    HttpResponseMessage response;
                    try
                    {
                        HttpRequestMessage message1 = new HttpRequestMessage(HttpMethod.Get, this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId);
                        response = HttpUtils.client.Send(message1);

                        // add to cart
                        if (response.Content.Headers.ContentLength == 0)
                        {
                            this.logger.LogWarning("Customer {0}: Response content for seller {1} product {2} is empty!", this.customerId, entry.sellerId, entry.productId);
                            return;
                        }

                        this.logger.LogWarning("Customer {0}: seller {1} product {2} retrieved", this.customerId, entry.sellerId, entry.productId);

                        var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);

                        var stream = response.Content.ReadAsStream();

                        StreamReader reader = new StreamReader(stream);
                        string productRet = reader.ReadToEnd();
                        var payload = BuildCartItem(productRet, qty);

                        HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/add");
                        message2.Content = payload;

                        this.logger.LogWarning("Customer {0}: Sending seller {1} product {2} payload to cart...", this.customerId, entry.sellerId, entry.productId);
                        response = HttpUtils.client.Send(message2);
                    }
                    catch (Exception e)
                    {
                        this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ",  customerId, this.config.productUrl, entry.sellerId, entry.productId, e.Message);
                    }

                });

                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                // artificial delay after adding the product
                await Task.Delay(delay);

            }

        }

        private async Task Browse(ISet<(long sellerId, long productId)> keyMap)
        {
            int delay;
            foreach (var entry in keyMap)
            {
                this.logger.LogWarning("Customer {0} browsing seller {1} product {2}", this.customerId, entry.sellerId, entry.productId);
                delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                await Task.Run(async () =>
                {
                    try
                    {
                        await HttpUtils.client.GetAsync(this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId); 
                    }
                    catch (Exception e)
                    {
                        this.logger.LogWarning("Exception Message: {0} Customer {1} Url {2} Seller {3} Product {4}", e.Message, customerId, this.config.productUrl, entry.sellerId, entry.productId);
                    }

                });
                // artificial delay after retrieving the product
                await Task.Delay(delay);
            }
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
                    product.freight_value,
                    quantity,
                    vouchers
            );
            var payload = JsonConvert.SerializeObject(basketItem);
            return HttpUtils.BuildPayload(payload);
        }

        private StringContent BuildCheckoutPayload(int tid, Customer customer)
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
                random.Next(1, 11), // installments
                tid
            );

            var payload = JsonConvert.SerializeObject(basketCheckout);
            return HttpUtils.BuildPayload(payload);
        }

    }
}