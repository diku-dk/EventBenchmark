using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Streaming;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Statefun.Workers;

public sealed class StatefunSellerThread : AbstractSellerThread
{
    private readonly HttpClient httpClient;

    public StatefunSellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
    {
        this.httpClient = httpClient;
    }

    public static StatefunSellerThread BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_" + sellerId);
        return new StatefunSellerThread(sellerId, httpClientFactory.CreateClient(), workerConfig, logger);
    }

    protected override void SendUpdatePriceRequest(string tid, Product productToUpdate, float newPrice)
    {
        string serializedObject = JsonConvert.SerializeObject(new PriceUpdate(this.sellerId, productToUpdate.product_id, newPrice, tid));

        string productId = this.sellerId + "-" + productToUpdate.product_id;

        HttpRequestMessage request = new(HttpMethod.Put, config.productUrl + "/" + productId)
        {
            Content = HttpUtils.BuildPayload(serializedObject, "application/vnd.marketplace/UpdatePrice")
        };

        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.PRICE_UPDATE, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
        }
    }

    protected override void SendProductUpdateRequest(Product product, string tid)
    {
        var obj = JsonConvert.SerializeObject(product);

        string productId = this.sellerId + "-" + product.product_id;

        HttpRequestMessage message = new(HttpMethod.Put, config.productUrl + "/" + productId)
        {
            Content = HttpUtils.BuildPayload(obj, "application/vnd.marketplace/UpsertProduct")
        };

        var now = DateTime.UtcNow;
        var resp = httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);

        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.UPDATE_PRODUCT, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogError("Seller {0} failed to update product {1} version: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }

    }

    public override void BrowseDashboard(string tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Put, config.sellerUrl + "/" + this.sellerId)
            {
                Content = new StringContent("{ \"tid\" : " + tid + " }", System.Text.Encoding.UTF8, "application/vnd.marketplace/QueryDashboard")
            };

            var now = DateTime.UtcNow;
            var response = httpClient.Send(message);
            if (response.IsSuccessStatusCode)
            {
                this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, now));
            }
            else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.QUERY_DASHBOARD, this.sellerId, MarkStatus.ABORT, "seller"));
                this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, response.ReasonPhrase);
            }
        }
        catch (Exception e)
        {
            this.logger.LogDebug("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
        }
    }

}


