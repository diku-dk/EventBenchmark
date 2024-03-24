using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace Common.Workers.Seller;

/**
 * The default seller thread assumes that updates to product are completed eventually but seller dashboard are completed synchronously
 */
public class DefaultSellerWorker : AbstractSellerWorker
{
    protected readonly HttpClient httpClient;

	protected DefaultSellerWorker(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, workerConfig, logger)
	{
        this.httpClient = httpClient;
	}

	public static DefaultSellerWorker BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);
        return new DefaultSellerWorker(sellerId, httpClientFactory.CreateClient(), workerConfig, logger);
    }

    protected override void SendUpdatePriceRequest(Product product, string tid)
    {
        HttpRequestMessage request = new(HttpMethod.Patch, this.config.productUrl);
        string serializedObject = JsonConvert.SerializeObject(new PriceUpdate(this.sellerId, product.product_id, product.price, tid));
        request.Content = HttpUtils.BuildPayload(serializedObject);

        var initTime = DateTime.UtcNow;
        var resp = this.httpClient.Send(request, HttpCompletionOption.ResponseHeadersRead);
        if (resp.IsSuccessStatusCode)
        {
            this.DoAfterSuccessUpdate(tid);
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.PRICE_UPDATE, initTime));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.PRICE_UPDATE, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogDebug("Seller {0} failed to update product {1} price: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }
    }

    protected override void SendProductUpdateRequest(Product product, string tid)
    {
        var obj = JsonConvert.SerializeObject(product);
        HttpRequestMessage message = new(HttpMethod.Put, this.config.productUrl)
        {
            Content = HttpUtils.BuildPayload(obj)
        };

        var now = DateTime.UtcNow;
        var resp = this.httpClient.Send(message, HttpCompletionOption.ResponseHeadersRead);

        if (resp.IsSuccessStatusCode)
        {
            this.DoAfterSuccessUpdate(tid);
            this.submittedTransactions.Add(new TransactionIdentifier(tid, TransactionType.UPDATE_PRODUCT, now));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.UPDATE_PRODUCT, this.sellerId, MarkStatus.ABORT, "product"));
            this.logger.LogDebug("Seller {0} failed to update product {1} version: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }
       
    }

    protected virtual void DoAfterSuccessUpdate(string tid)
    {
        // do nothing by default
    }

    public override void BrowseDashboard(string tid)
    {
        try
        {
            HttpRequestMessage message = new(HttpMethod.Get, config.sellerUrl + "/dashboard/" + this.sellerId);

            var now = DateTime.UtcNow;
            var response = httpClient.Send(message);
            if (response.IsSuccessStatusCode)
            {
                this.finishedTransactions.Add(new TransactionOutput(tid, DateTime.UtcNow));
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

