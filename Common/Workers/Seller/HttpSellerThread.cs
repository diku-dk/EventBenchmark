using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Dapr.Workers;

public sealed class HttpSellerThread : AbstractSellerThread
{
    private readonly HttpClient httpClient;

	public HttpSellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
	{
        this.httpClient = httpClient;
	}

	public static HttpSellerThread BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);
        return new HttpSellerThread(sellerId, httpClientFactory.CreateClient(), workerConfig, logger);
    }

    protected override void SendUpdatePriceRequest(string tid, Product productToUpdate, float newPrice)
    {
        HttpRequestMessage request = new(HttpMethod.Patch, config.productUrl);
        string serializedObject = JsonConvert.SerializeObject(new PriceUpdate(this.sellerId, productToUpdate.product_id, newPrice, tid));
        request.Content = HttpUtils.BuildPayload(serializedObject);

        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
        }
        else
        {
            this.logger.LogDebug("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
        }
    }

    protected override void SendProductUpdateRequest(Product product, string tid)
    {
        var obj = JsonConvert.SerializeObject(product);
        HttpRequestMessage message = new(HttpMethod.Put, config.productUrl)
        {
            Content = HttpUtils.BuildPayload(obj)
        };

        var now = DateTime.UtcNow;
        var resp = httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);

        if (resp.IsSuccessStatusCode)
        {
             this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
        }
        else
        {
            this.logger.LogDebug("Seller {0} failed to update product {1} version: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }
       
    }

    public override void BrowseDashboard(string tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Get, config.sellerUrl + "/" + this.sellerId);
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, DateTime.UtcNow));
            var response = httpClient.Send(message);
            if (response.IsSuccessStatusCode)
            {
                this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
            }
            else
            {
                this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, response.ReasonPhrase);
            }
        }
        catch (Exception e)
        {
            this.logger.LogDebug("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
        }
    }

    public override void AddFinishedTransaction(TransactionOutput transactionOutput){}
}

