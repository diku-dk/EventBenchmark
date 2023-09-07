using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Requests;
using Common.Workers;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Newtonsoft.Json;

namespace Dapr.Workers;

public sealed class DaprSellerThread : AbstractSellerThread
{
	public DaprSellerThread(int sellerId, HttpClient httpClient, SellerWorkerConfig workerConfig, ILogger logger) : base(sellerId, httpClient, workerConfig, logger)
	{
	}

	public static DaprSellerThread BuildSellerThread(int sellerId, IHttpClientFactory httpClientFactory, SellerWorkerConfig workerConfig)
    {
        var logger = LoggerProxy.GetInstance("SellerThread_"+ sellerId);
        var httpClient = httpClientFactory.CreateClient();
        return new DaprSellerThread(sellerId, httpClient, workerConfig, logger);
    }

    protected override void SendUpdatePriceRequest(int tid, Product productToUpdate, float newPrice)
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
            this.logger.LogError("Seller {0} failed to update product {1} price: {2}", this.sellerId, productToUpdate.product_id, resp.ReasonPhrase);
        }
    }

    protected override void SendProductUpdateRequest(Product product, int tid)
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
            this.logger.LogError("Seller {0} failed to update product {1} version: {2}", this.sellerId, product.product_id, resp.ReasonPhrase);
        }
       
    }

}


