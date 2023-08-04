using System.Text;
using Common.Http;
using Common.Workload.Customer;
using Common.Entities;
using Common.Streaming;
using Grains.WorkerInterfaces;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Streams;
using Common.Distribution;
using Common.Requests;
using Common.Workload;
using Common.Workload.Metrics;
using Orleans.Concurrency;
using MathNet.Numerics.Distributions;
using System.Net;

namespace Grains.Workers
{

    [Reentrant]
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {
        private readonly Random random;

        private CustomerWorkerConfig config;

        private IDiscreteDistribution sellerIdGenerator;

        // the customer this worker is simulating
        private int customerId;

        // the object respective to this worker
        private Customer customer;

        private readonly LinkedList<TransactionIdentifier> submittedTransactions;

        private readonly HttpClient httpClient;

        private readonly ILogger<CustomerWorker> logger;

        public CustomerWorker(HttpClient httpClient, ILogger<CustomerWorker> logger)
        {
            this.httpClient = httpClient;
            this.logger = logger;
            this.random = new Random();
            this.submittedTransactions = new LinkedList<TransactionIdentifier>();
        }

        public override async Task OnActivateAsync(CancellationToken cancellationToken)
        {
            this.customerId = (int) this.GetPrimaryKeyLong();// await base.OnActivateAsync(cancellationToken);
            var streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.CustomerWorkerNameSpace, this.customerId.ToString());
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

        public Task Init(CustomerWorkerConfig config, Customer customer)
        {
            this.logger.LogInformation("Customer worker {0} Init", this.customerId);
            this.config = config;
            this.customer = customer;
            this.sellerIdGenerator =
                this.config.sellerDistribution == DistributionType.UNIFORM ?
                new DiscreteUniform(this.config.sellerRange.min, this.config.sellerRange.max, new Random()) :
                new Zipf(0.80, this.config.sellerRange.max, new Random());

            this.submittedTransactions.Clear();

            return Task.CompletedTask;
        }

        public async Task Run(int tid, StreamSequenceToken token)
        {
            await this.Run(tid);
        }

        public async Task Run(int tid)//, StreamSequenceToken token)
        {
            this.logger.LogInformation("Customer worker {0} starting new TID: {1}", this.customerId, tid);

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            this.logger.LogInformation("Customer {0} number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            if (config.interactive)
            {
                var keyMap = await DefineKeysToBrowseAsync(numberOfKeysToBrowse);

                await Browse(keyMap);

                // TODO should we also model this behavior?
                int numberOfKeysToCheckout =
                    random.Next(1, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);

                // adding to cart
                try
                {
                    var res = DefineKeysToCheckout(keyMap.ToList(), numberOfKeysToCheckout);
                    await AddItemsToCartFromSet(res);
                    GetCart();
                    Checkout(tid);
                }
                catch (Exception e)
                {
                    this.logger.LogError(e.Message);
                }
            }
            else
            {
                int numberOfKeysToCheckout = random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
                var products = await DefineProductsToCheckout(numberOfKeysToCheckout);
                await AddItemsToCart(products);
                Checkout(tid);
            }
        }

        /**
         * From the list of browsed keys, picks randomly the keys to checkout
         */
        private ISet<(int sellerId, int productId)> DefineKeysToCheckout(List<(int sellerId, int productId)> browsedKeys, int numberOfKeysToCheckout)
        {
            ISet<(int sellerId, int productId)> set = new HashSet<(int sellerId, int productId)>(numberOfKeysToCheckout);
            while (set.Count < numberOfKeysToCheckout)
            {
                set.Add(browsedKeys[random.Next(0, browsedKeys.Count)]);
            }
            return set;
        }

        private async Task<List<Product>> DefineProductsToCheckout(int numberOfProducts)
        {
            List<Product> list = new(numberOfProducts);
            ISellerWorker sellerWorker;
            List<Task<Product>> tasksToAwait = new();
            for (int i = 0; i < numberOfProducts; i++)
            {
                var sellerId = this.sellerIdGenerator.Sample();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);

                tasksToAwait.Add(sellerWorker.GetProduct());
            }
            await Task.WhenAll(tasksToAwait);
            foreach(var task in tasksToAwait)
            {
                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
                list.Add(task.Result);
            }
            return list;
        }

        private async Task<ISet<(int sellerId, int productId)>> DefineKeysToBrowseAsync(int numberOfKeysToBrowse)
        {
            ISet<(int sellerId, int productId)> keyMap = new HashSet<(int sellerId, int productId)>(numberOfKeysToBrowse);
            ISellerWorker sellerWorker;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                var sellerId = this.sellerIdGenerator.Sample();
                sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);

                // we dont measure the performance of the benchmark, only the system. as long as we can submit enough workload we are fine
                var productId = await sellerWorker.GetProductId();
                while (keyMap.Contains((sellerId,productId)))
                {
                    sellerId = this.sellerIdGenerator.Sample();
                    sellerWorker = GrainFactory.GetGrain<ISellerWorker>(sellerId);
                    productId = await sellerWorker.GetProductId();
                }

                keyMap.Add((sellerId,productId));
                sb.Append(sellerId).Append("-").Append(productId);
                if (i < numberOfKeysToBrowse - 1) sb.Append(" | ");
            }
            this.logger.LogInformation("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());
            return keyMap;
        }

        /**
         * Simulating the customer browsing the cart before checkout
         * could verify whether the cart contains all the products chosen, otherwise throw exception
         */
        private void GetCart()
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, this.config.cartUrl + "/" + this.customerId);
            httpClient.Send(message);
        }

        private void Checkout(int tid)
        {
            // define whether client should send a checkout request
            if (random.Next(0, 100) > this.config.checkoutProbability)
            {
                InformFailedCheckout();
                this.logger.LogInformation("Customer {0} decided to not send a checkout.", this.customerId);
                return;
            }

            this.logger.LogInformation("Customer {0} decided to send a checkout", this.customerId);
            // inform checkout intent. optional feature
            var payload = BuildCheckoutPayload(tid, this.customer);
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, this.config.cartUrl + "/" + this.customerId + "/checkout");
            message.Content = payload;

            var now1 = DateTime.UtcNow;
            HttpResponseMessage resp = httpClient.Send(message);
            if (resp.IsSuccessStatusCode)
            {
                TransactionIdentifier txId = new TransactionIdentifier(tid, TransactionType.CUSTOMER_SESSION, now1);
                submittedTransactions.AddLast(txId);
            }
            // TODO add config param Resubmit on Checkout Reject
            else if (resp.StatusCode == HttpStatusCode.MethodNotAllowed)
            {
                var now2 = DateTime.UtcNow;
                message = new HttpRequestMessage(HttpMethod.Post, this.config.cartUrl + "/" + this.customerId + "/checkout");
                message.Content = payload;
                resp = httpClient.Send(message);
                if (resp.IsSuccessStatusCode)
                {
                    TransactionIdentifier txId = new TransactionIdentifier(tid, TransactionType.CUSTOMER_SESSION, now2);
                    submittedTransactions.AddLast(txId);
                }
            }
            else
            {
                var now = DateTime.UtcNow;
                logger.LogDebug("Customer {0} failed checkout for TID {0} at {1}. Status {2}", customerId, tid, now, resp.StatusCode);
                InformFailedCheckout();
            }
        }

        private async Task AddItemsToCart(List<Product> products)
        {
            foreach(var product in products)
            {
                this.logger.LogInformation("Customer {0}: Adding seller {1} product {2} to cart", this.customerId, product.seller_id, product.product_id);
                HttpResponseMessage response;
                var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
                var payload = BuildCartItem(product, qty);
                try
                {
                    HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/add");
                    message2.Content = payload;
                    response = httpClient.Send(message2);
                }
                catch (Exception e)
                {
                    this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ", customerId, this.config.productUrl, product.seller_id, product.product_id, e.Message);
                }
                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                if(delay > 0)
                    await Task.Delay(delay);
            }
        }

        private async Task AddItemsToCartFromSet(ISet<(int sellerId, int productId)> keyMap)
        {
            foreach (var entry in keyMap)
            {
                this.logger.LogInformation("Customer {0}: Adding seller {1} product {2} to cart", this.customerId, entry.sellerId, entry.productId);
                HttpResponseMessage response;
                try
                {
                    HttpRequestMessage message1 = new HttpRequestMessage(HttpMethod.Get, this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId);
                    response = httpClient.Send(message1);

                    // add to cart
                    if (response.Content.Headers.ContentLength == 0)
                    {
                        this.logger.LogWarning("Customer {0}: Response content for seller {1} product {2} is empty!", this.customerId, entry.sellerId, entry.productId);
                        return;
                    }

                    this.logger.LogInformation("Customer {0}: seller {1} product {2} retrieved", this.customerId, entry.sellerId, entry.productId);

                    var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);

                    var stream = response.Content.ReadAsStream();

                    StreamReader reader = new StreamReader(stream);
                    string productRet = reader.ReadToEnd();
                    var payload = BuildCartItem(productRet, qty);

                    HttpRequestMessage message2 = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/add");
                    message2.Content = payload;

                    this.logger.LogInformation("Customer {0}: Sending seller {1} product {2} payload to cart...", this.customerId, entry.sellerId, entry.productId);
                    response = httpClient.Send(message2);
                }
                catch (Exception e)
                {
                    this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ",  customerId, this.config.productUrl, entry.sellerId, entry.productId, e.Message);
                }
                int delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                // artificial delay after adding the product
                if(delay > 0)
                    await Task.Delay(delay);
            }

        }

        private async Task Browse(ISet<(int sellerId, int productId)> keyMap)
        {
            this.logger.LogInformation("Customer {0} started browsing...", this.customerId);
            int delay;
            foreach (var entry in keyMap)
            {
                this.logger.LogInformation("Customer {0} browsing seller {1} product {2}", this.customerId, entry.sellerId, entry.productId);
                delay = this.random.Next(this.config.delayBetweenRequestsRange.min, this.config.delayBetweenRequestsRange.max + 1);
                HttpRequestMessage msg = new HttpRequestMessage(HttpMethod.Get, this.config.productUrl + "/" + entry.sellerId + "/" + entry.productId);
                httpClient.Send(msg);
                // artificial delay after retrieving the product
                await Task.Delay(delay);
            }
            this.logger.LogInformation("Customer worker {0} finished browsing.", this.customerId);
        }

        private StringContent BuildCartItem(string productPayload, int quantity)
        {
            Product product = JsonConvert.DeserializeObject<Product>(productPayload);
            return BuildCartItem(product, quantity);
        }

        private StringContent BuildCartItem(Product product, int quantity)
        {
            // define voucher from distribution
            var vouchers = Array.Empty<float>();
            int probVoucher = this.random.Next(0, 101);
            if(probVoucher <= this.config.voucherProbability)
            {
                int numVouchers = this.random.Next(1, this.config.maxNumberVouchers + 1);
                vouchers = new float[numVouchers];
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

        private void InformFailedCheckout()
        {
            // just cleaning cart state for next browsing
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/seal");
            httpClient.Send(message);
        }

        public Task<List<TransactionIdentifier>> Collect()
        {
            return Task.FromResult(submittedTransactions.ToList());
        }

    }
}