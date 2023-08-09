using System.Threading.Channels;
using Common.Infra;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;

namespace Daprr.Workers;

public class DeliveryThread
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
        var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
        var resp = httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            var endTime = DateTime.UtcNow;
            var end = new TransactionOutput(tid, endTime);
            while (!ResultQueue.Writer.TryWrite((init, end))) { }
        }
    }
}


