using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using System.Text;

namespace Common.Workers;

public class StatefunDeliveryThread : DeliveryThread
{

    new public static StatefunDeliveryThread BuildDeliveryThread(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new StatefunDeliveryThread(config, httpClientFactory.CreateClient(), logger);
    }

    private StatefunDeliveryThread(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger) : base(config,httpClient,logger)
    {
    }

	new public void Run(string tid)
	{
        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Put, config.shipmentUrl + "/" + tid)
        {
            Content = new StringContent("{ \"tid\" : " + tid + " }", Encoding.UTF8, "application/vnd.marketplace/UpdateShipment")
        };

        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
            var endTime = DateTime.UtcNow;
            var end = new TransactionOutput(tid, endTime);
            this.resultQueue.Add((init, end));
        } else
        {
            this.abortedTransactions.Add( new TransactionMark(tid, TransactionType.UPDATE_DELIVERY, 1, MarkStatus.ABORT, "shipment")  );
            this.logger.LogDebug("Delivery worker failed to update delivery for TID {0}: {1}", tid, resp.ReasonPhrase);
        }
    }

}
