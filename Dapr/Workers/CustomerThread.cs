using System.Net;
using Common.Distribution;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Daprr.Services;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;

namespace Daprr.Workrs;

public sealed class CustomerThread : ICustomerWorker
{  
    private readonly Random random;

    private CustomerWorkerConfig config;

    private IDiscreteDistribution sellerIdGenerator;

    // the customer this worker is simulating
    private int customerId;

    // the object respective to this worker
    private Customer customer;

    private readonly List<TransactionIdentifier> submittedTransactions;

    private readonly HttpClient httpClient;

    private readonly ISellerService sellerService;

    private readonly ILogger logger;

    public static CustomerThread BuildCustomerThread(IHttpClientFactory httpClientFactory, ISellerService sellerService, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
        return new CustomerThread(sellerService, config, customer, httpClientFactory.CreateClient(), logger);
    }

    private CustomerThread(ISellerService sellerService, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger)
    {
        this.sellerService = sellerService;
        this.httpClient = httpClient;
        this.config = config;
        this.customer = customer;
        this.sellerIdGenerator =
            this.config.sellerDistribution == DistributionType.UNIFORM ?
            new DiscreteUniform(this.config.sellerRange.min, this.config.sellerRange.max, new Random()) :
            new Zipf(0.80, this.config.sellerRange.max, new Random());

        this.logger = logger;
        this.submittedTransactions = new();
        this.random = new Random();
    }

    public void Run(int tid)
    {
        int numberOfProducts = random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
        List<Product> products = GetProductsToCheckout(numberOfProducts);
        AddItemsToCart(products);
        Checkout(tid);
    }

    private List<Product> GetProductsToCheckout(int numberOfProducts)
    {
        ISet<(int, int)> set = new HashSet<(int,int)>();
        List<Product> list = new(numberOfProducts);
        
        for (int i = 0; i < numberOfProducts; i++)
        {
            var sellerId = this.sellerIdGenerator.Sample();
            var product = sellerService.GetProduct(sellerId);

            while (set.Contains((sellerId, product.product_id)))
            {
                sellerId = this.sellerIdGenerator.Sample();
                product = sellerService.GetProduct(sellerId);
            }
            list.Add(product);
        }
        return list;
    }

    private void AddItemsToCart(List<Product> products)
    {
        foreach (var product in products)
        {            
            var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
            var payload = BuildCartItem(product, qty);
            try
            {
                HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/add")
                {
                    Content = payload
                };
                httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
            }
            catch (Exception e)
            {
                this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ", customerId, this.config.productUrl, product.seller_id, product.product_id, e.Message);
            }
            
        }
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
        HttpRequestMessage message = new(HttpMethod.Post, this.config.cartUrl + "/" + this.customerId + "/checkout");
        message.Content = payload;

        var now1 = DateTime.UtcNow;
        HttpResponseMessage resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, now1);
            submittedTransactions.Add(txId);
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
                TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, now2);
                submittedTransactions.Add(txId);
            }
        }
        else
        {
            var now = DateTime.UtcNow;
            logger.LogDebug("Customer {0} failed checkout for TID {0} at {1}. Status {2}", customerId, tid, now, resp.StatusCode);
            InformFailedCheckout();
        }
    }

    private void InformFailedCheckout()
    {
        // just cleaning cart state for next browsing
        HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customerId + "/seal");
        httpClient.Send(message);
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

    private StringContent BuildCartItem(Product product, int quantity)
    {
        // define voucher from distribution
        var vouchers = Array.Empty<float>();
        int probVoucher = this.random.Next(0, 101);
        if (probVoucher <= this.config.voucherProbability)
        {
            int numVouchers = this.random.Next(1, this.config.maxNumberVouchers + 1);
            vouchers = new float[numVouchers];
            for (int i = 0; i < numVouchers; i++)
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

}