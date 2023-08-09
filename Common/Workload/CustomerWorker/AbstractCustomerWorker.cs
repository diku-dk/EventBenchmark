using Common.Distribution;
using Common.Entities;
using Common.Http;
using Common.Requests;
using Common.Workload.Metrics;
using MathNet.Numerics.Distributions;
using Newtonsoft.Json;

namespace Common.Workload.CustomerWorker;

public abstract class AbstractCustomerWorker
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

    public AbstractCustomerWorker(HttpClient httpClient, CustomerWorkerConfig config, Customer customer)
    {
        this.httpClient = httpClient;
        this.config = config;
        this.customer = customer;
        this.sellerIdGenerator =
            this.config.sellerDistribution == DistributionType.UNIFORM ?
            new DiscreteUniform(this.config.sellerRange.min, this.config.sellerRange.max, new Random()) :
            new Zipf(0.80, this.config.sellerRange.max, new Random());
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

}

