using System.Threading.Channels;
using Common.Infra;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Common.Workers;

public class DeliveryThread : IDeliveryWorker
{
    private readonly HttpClient httpClient;
    private readonly DeliveryWorkerConfig config;

    private readonly ILogger logger;

    private readonly Channel<(TransactionIdentifier, TransactionOutput)> ResultQueue;

    public static DeliveryThread BuildDeliveryThread(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new DeliveryThread(config, httpClientFactory.CreateClient(), logger);
    }

    private DeliveryThread(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.logger = logger;
        this.ResultQueue = Channel.CreateUnbounded<(TransactionIdentifier, TransactionOutput)>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    }

	public void Run(int tid)
	{
        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, config.shipmentUrl + "/" + tid);
        var initTime = DateTime.UtcNow;
        var resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
            var endTime = DateTime.UtcNow;
            var end = new TransactionOutput(tid, endTime);
            while (!ResultQueue.Writer.TryWrite((init, end))) { }
        } else
        {
            this.logger.LogError("Delivery worker failed to update delivery for TID {0}: {1}", tid, resp.ReasonPhrase);
        }
    }

    public List<(TransactionIdentifier, TransactionOutput)> GetResults()
    {
        var list = new List<(TransactionIdentifier, TransactionOutput)>();
        while (ResultQueue.Reader.TryRead(out (TransactionIdentifier, TransactionOutput) item))
        {
            list.Add(item);
        }
        return list;
    }

}

