using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Services;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Common.Workers.Customer;

public class HttpCustomerThread : AbstractCustomerThread
{
    protected readonly HttpClient httpClient;
    private readonly ISet<(int, int)> cartItems;

    protected HttpCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Entities.Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, logger)
    {
        this.httpClient = httpClient;
        this.cartItems = new HashSet<(int, int)>(config.maxNumberKeysToAddToCart);
    }

    public static HttpCustomerThread BuildCustomerThread(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Entities.Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
        return new HttpCustomerThread(sellerService, numberOfProducts, config, customer, httpClientFactory.CreateClient(), logger);
    }

    public override List<TransactionOutput> GetFinishedTransactions()
    {
        throw new NotImplementedException();
    }

    public override void AddItemsToCart()
    {
        int numberKeysToAddToCart = this.random.Next(1, this.config.maxNumberKeysToAddToCart + 1);
        while (cartItems.Count < numberKeysToAddToCart)
        {
            AddItem();
        }
        // clean it so garbage collector can collect the items
        this.cartItems.Clear();
    }

    private void AddItem()
    {
        //logger.LogWarning("Adding item");
        var sellerId = this.sellerIdGenerator.Sample();
        var product = sellerService.GetProduct(sellerId, this.productIdGenerator.Sample() - 1);
        if (this.cartItems.Add((sellerId, product.product_id)))
        {
            //logger.LogWarning("Entered Adding item");
            var qty = this.random.Next(this.config.minMaxQtyRange.min, this.config.minMaxQtyRange.max + 1);
            try
            {
                var payload = this.BuildCartItem(product, qty);
                HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customer.id + "/add")
                {
                    Content = payload
                };
                this.httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
            }
            catch (Exception e)
            {
                this.logger.LogError("Customer {0} Url {1} Seller {2} Key {3}: Exception Message: {5} ", customer.id, this.config.productUrl, product.seller_id, product.product_id, e.Message);
            }
        }
    }

    protected override void InformFailedCheckout()
    {
        // just cleaning cart state for next browsing
        HttpRequestMessage message = new(HttpMethod.Patch, this.config.cartUrl + "/" + customer.id + "/seal");
        try{ this.httpClient.Send(message); } catch(Exception){ }
    }

    protected override void SendCheckoutRequest(string tid)
    {
        var payload = BuildCheckoutPayload(tid);
        string url = this.config.cartUrl + "/" + this.customer.id + "/checkout";
        HttpRequestMessage message = new(HttpMethod.Post, url)
        {
            Content = payload
        };

        var sentTs = DateTime.UtcNow;
        try
        {
            HttpResponseMessage resp = httpClient.Send(message);
            if (resp.IsSuccessStatusCode)
            {
                TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, sentTs);
                this.submittedTransactions.Add(txId);
                DoAfterSubmission(tid);
            }
            else
            {
                 InformFailedCheckout();
            }
        }
        catch (Exception e)
        {
            this.logger.LogError("Customer {0} Url {1}: Exception Message: {5} ", customer.id, url, e.Message);
            InformFailedCheckout();
        }
    }

    protected virtual void DoAfterSubmission(string tid)
    {
    }

    protected StringContent BuildCheckoutPayload(string tid)
    {
        // define payment type randomly
        var typeIdx = random.Next(1, 4);
        PaymentType type = typeIdx > 2 ? PaymentType.CREDIT_CARD : typeIdx > 1 ? PaymentType.DEBIT_CARD : PaymentType.BOLETO;
        int installments = type == PaymentType.CREDIT_CARD ? random.Next(1, 11) : 0;

        // build
        CustomerCheckout customerCheckout = new CustomerCheckout(
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
            installments,
            tid
        );

        var payload = JsonConvert.SerializeObject(customerCheckout);
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

        // build a cart item
        CartItem cartItem = new CartItem(
                product.seller_id,
                product.product_id,
                product.name,
                product.price,
                product.freight_value,
                quantity,
                voucher,
                product.version
        );

        var payload = JsonConvert.SerializeObject(cartItem);
        return HttpUtils.BuildPayload(payload);
    }

}

