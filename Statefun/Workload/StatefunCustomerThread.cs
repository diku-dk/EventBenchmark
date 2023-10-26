using Common.Distribution;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Services;
using Common.Streaming;
using Common.Workers.Customer;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Orleans.Workload;

public class StatefunCustomerThread : HttpCustomerThread
{
    protected readonly List<TransactionOutput> finishedTransactions;

    private StatefunCustomerThread(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, httpClient, logger)
    {
        this.finishedTransactions = new List<TransactionOutput>();
    }

    public static new StatefunCustomerThread BuildCustomerThread(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer" + customer.id.ToString());
        return new StatefunCustomerThread(sellerService, numberOfProducts, config, customer, httpClientFactory.CreateClient(), logger);
    }

    public override List<TransactionOutput> GetFinishedTransactions()
    {
        return this.finishedTransactions;
    }

    public override void SetUp(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution)
    {
        base.SetUp(sellerDistribution, sellerRange, keyDistribution);
        this.finishedTransactions.Clear();
    }

    protected override void BuildAddCartPayloadAndSend(string objStr)
    {
        var payload = HttpUtils.BuildPayload(objStr, "application/vnd.marketplace/AddCartItem");
        HttpRequestMessage message = new(HttpMethod.Put, this.config.cartUrl + "/" + this.customer.id)
        {
            Content = payload
        };
        this.httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);
    }

    protected override void SendCheckoutRequest(string tid)
    {
        var objStr = BuildCheckoutPayload(tid);

        var payload = HttpUtils.BuildPayload(objStr, "application/vnd.marketplace/CustomerCheckout");
       
        try
        {
            DateTime sentTs = DateTime.UtcNow;
            HttpRequestMessage message = new(HttpMethod.Put, this.config.cartUrl + "/" + this.customer.id)
            {
                Content = payload
            };

            HttpResponseMessage resp = httpClient.Send(message);

            if (resp.IsSuccessStatusCode)
            {
                TransactionIdentifier txId = new(tid, TransactionType.CUSTOMER_SESSION, sentTs);
                this.submittedTransactions.Add(txId);
            }
            else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.CUSTOMER_SESSION, this.customer.id, MarkStatus.ABORT, "cart"));
            }
        }
        catch (Exception e)
        {
            this.logger.LogError("Customer {0} Url {1}: Exception Message: {5} ", customer.id, this.config.cartUrl + "/" + this.customer.id, e.Message);
            InformFailedCheckout();
        }
    }

    protected override void InformFailedCheckout()
    {
        HttpRequestMessage message = new(HttpMethod.Put, this.config.cartUrl + "/" + this.customer.id)
        {
            Content = new StringContent("{}", Encoding.UTF8, "application/vnd.marketplace/Seal")
        };
        try { this.httpClient.Send(message); } catch (Exception) { }
    }

}