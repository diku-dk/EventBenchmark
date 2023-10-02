using System.Collections.Concurrent;
using Common.Infra;
using Common.Streaming;
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

    private readonly BlockingCollection<(TransactionIdentifier, TransactionOutput)> resultQueue;
    private readonly ConcurrentBag<TransactionMark> abortedTransactions;

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
        this.resultQueue = new BlockingCollection<(TransactionIdentifier, TransactionOutput)>();
        this.abortedTransactions = new();
    }

	public void Run(string tid)
	{
        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, config.shipmentUrl + "/" + tid);
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

    public List<(TransactionIdentifier, TransactionOutput)> GetResults()
    {
        var list = new List<(TransactionIdentifier, TransactionOutput)>();
        while (this.resultQueue.TryTake(out (TransactionIdentifier, TransactionOutput) item))
        {
            list.Add(item);
        }
        return list;
    }

    public List<TransactionMark> GetAbortedTransactions()
    {
        var list = new List<TransactionMark>();
        while (this.abortedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }

}

