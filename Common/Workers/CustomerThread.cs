using Common.Distribution;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Services;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Common.Workload.Workers;
using Common.Workload.CustomerWorker;

namespace Common.Workers;

public class CustomerThread : ICustomerWorker
{  
    private readonly Random random;

    private CustomerWorkerConfig config;

    private IDiscreteDistribution sellerIdGenerator;
    private readonly int numberOfProducts;
    private IDiscreteDistribution productIdGenerator;

    // the object respective to this worker
    private Customer customer;

    private readonly List<TransactionIdentifier> submittedTransactions;

    private readonly HttpClient httpClient;

    private readonly ISellerService sellerService;

    private readonly ILogger logger;

    public static CustomerThread BuildCustomerThread(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
        return new CustomerThread(sellerService, numberOfProducts, config, customer, httpClientFactory.CreateClient(), logger);
    }

    private CustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger)
    {
        this.sellerService = sellerService;
        this.httpClient = httpClient;
        this.config = config;
        this.customer = customer;
        this.numberOfProducts = numberOfProducts;
        this.logger = logger;
        this.submittedTransactions = new();
        this.random = new Random();
    }

    public void SetDistribution(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        this.sellerIdGenerator = sellerDistribution == DistributionType.UNIFORM ?
                                  new DiscreteUniform(sellerRange.min, sellerRange.max, new Random()) :
                                  new Zipf(0.80, sellerRange.max, new Random());
        this.productIdGenerator = keyDistribution == DistributionType.UNIFORM ?
                                new DiscreteUniform(1, numberOfProducts, new Random()) :
                                new Zipf(0.99, numberOfProducts, new Random());
    }

    public void Run(int tid)
    {
        AddItemsToCart();
        Checkout(tid);
    }

    public void AddItemsToCart()
    {
        int numberOfProducts = random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
        ISet<(int, int)> set = new HashSet<(int, int)>();
        while (set.Count < numberOfProducts)
        {
            var sellerId = this.sellerIdGenerator.Sample();
            var product = sellerService.GetProduct(sellerId, this.productIdGenerator.Sample());
            if (set.Add((sellerId, product.product_id)))
            {
                var qty = random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
                var payload = BuildCartItem(product, qty);
                try
                {
                    HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customer.id + "/add")
                    {
                        Content = payload
                    };
                    this.httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
                }
                catch (Exception e)
                {
                    this.logger.LogWarning("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ", customer.id, this.config.productUrl, product.seller_id, product.product_id, e.Message);
                }
            }
        }
    }

    public void Checkout(int tid)
    {
        // define whether client should send a checkout request
        if (random.Next(0, 100) > this.config.checkoutProbability)
        {
            InformFailedCheckout();
            return;
        }

        // inform checkout intent. optional feature
        var payload = BuildCheckoutPayload(tid, this.customer);
        HttpRequestMessage message = new(HttpMethod.Post, this.config.cartUrl + "/" + this.customer.id + "/checkout");
        message.Content = payload;

        var now = DateTime.UtcNow;
        HttpResponseMessage resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, now);
            this.submittedTransactions.Add(txId);
        }
        else
        {
            InformFailedCheckout();
        }
    }

    private void InformFailedCheckout()
    {
        // just cleaning cart state for next browsing
        HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customer.id + "/seal");
        this.httpClient.Send(message);
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
        float voucher = 0;
        int probVoucher = this.random.Next(0, 101);
        if (probVoucher <= this.config.voucherProbability)
        {
            voucher = product.price * 0.10f;
        }

        // build a basket item
        CartItem basketItem = new CartItem(
                product.seller_id,
                product.product_id,
                product.name,
                product.price,
                product.freight_value,
                quantity,
                voucher,
                product.version
        );
        var payload = JsonConvert.SerializeObject(basketItem);
        return HttpUtils.BuildPayload(payload);
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.submittedTransactions;
    }

}