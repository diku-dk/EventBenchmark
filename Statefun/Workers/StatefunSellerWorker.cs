using Common.Entities;
using Common.Infra;
using Common.Requests;
using Common.Streaming;
using Common.Workers.Seller;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Statefun.Infra;

namespace Statefun.Workers;

public sealed class StatefunSellerWorker : AbstractSellerWorker
{
    private readonly HttpClient httpClient;

    private StatefunSellerWorker(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
    {
        this.httpClient = httpClient;
    }

    public static StatefunSellerWorker BuildSellerWorker(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_" + sellerId);
        return new StatefunSellerWorker(sellerId, httpClientFactory.CreateClient(), workerConfig, logger);
    }

    protected override void SendUpdatePriceRequest(Product product, string tid)
    {
        string payLoad = JsonConvert.SerializeObject(
            new PriceUpdate(this.sellerId, product.product_id, product.price, product.version, tid));
        string partitionID = this.sellerId + "-" + product.product_id;
        string apiUrl = string.Concat(this.config.productUrl, "/", partitionID);        
        string eventType = "UpdatePrice";
        string contentType = string.Concat(StatefunUtils.BASE_CONTENT_TYPE, eventType);
        HttpResponseMessage resp = StatefunUtils.SendHttpToStatefun(this.httpClient, apiUrl, contentType, payLoad).Result;    

        var initTime = DateTime.UtcNow;
        if (resp.IsSuccessStatusCode)
        {
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.PRICE_UPDATE, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }
    }

    protected override void SendProductUpdateRequest(Product product, string tid)
    {
        var payLoad = JsonConvert.SerializeObject(product);

        string partitionID = this.sellerId + "-" + product.product_id;

        string apiUrl = string.Concat(this.config.productUrl, "/", partitionID);        
        string eventType = "UpsertProduct";
        string contentType = string.Concat(StatefunUtils.BASE_CONTENT_TYPE, eventType);
        HttpResponseMessage resp = StatefunUtils.SendHttpToStatefun(this.httpClient, apiUrl, contentType, payLoad).Result;   

        var now = DateTime.UtcNow;

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
            string partitionID = this.sellerId.ToString();
            string apiUrl = string.Concat(this.config.sellerUrl, "/", partitionID);        
            string eventType = "QueryDashboard";
            string contentType = string.Concat(StatefunUtils.BASE_CONTENT_TYPE, eventType);
            string payLoad = "{ \"tid\" : " + tid + " }";
            HttpResponseMessage resp = StatefunUtils.SendHttpToStatefun(this.httpClient, apiUrl, contentType, payLoad).Result;   

            var now = DateTime.UtcNow;
            if (resp.IsSuccessStatusCode)
            {
                this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.QUERY_DASHBOARD, now));
            }
            else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.QUERY_DASHBOARD, this.sellerId, MarkStatus.ABORT, "seller"));
                this.logger.LogDebug("Seller {0}: Dashboard retrieval failed: {0}", this.sellerId, resp.ReasonPhrase);
            }
        }
        catch (Exception e)
        {
            this.logger.LogDebug("Seller {0}: Dashboard could not be retrieved: {1}", this.sellerId, e.Message);
        }
    }

    public override void AddFinishedTransaction(TransactionOutput transactionOutput)
    {
        this.finishedTransactions.Add(transactionOutput);
    }

}