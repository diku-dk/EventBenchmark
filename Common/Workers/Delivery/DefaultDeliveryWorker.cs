using System.Collections.Concurrent;
using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Common.Workers.Delivery;

/**
 * Default delivery worker. It considers a synchronous API for requesting an UPDATE DELIVERY transaction
 */
public class DefaultDeliveryWorker : IDeliveryWorker
{
    protected readonly HttpClient httpClient;

    protected readonly DeliveryWorkerConfig config;

    protected readonly ILogger logger;

    protected readonly BlockingCollection<(TransactionIdentifier, TransactionOutput)> resultQueue;

    protected readonly ConcurrentBag<TransactionMark> abortedTransactions;

    public static DefaultDeliveryWorker BuildDeliveryWorker(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new DefaultDeliveryWorker(config, httpClientFactory.CreateClient(), logger);
    }

    protected DefaultDeliveryWorker(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger)
    {
        this.config = config;
        this.httpClient = httpClient;
        this.logger = logger;
        this.resultQueue = new BlockingCollection<(TransactionIdentifier, TransactionOutput)>();
        this.abortedTransactions = new();
    }

    public void Run(string tid)
    {
        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, this.config.shipmentUrl + "/" + tid);
        var initTime = DateTime.UtcNow;
        var resp = this.httpClient.Send(message);
        if (resp.IsSuccessStatusCode)
        {
            var endTime = DateTime.UtcNow;
            var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
            var end = new TransactionOutput(tid, endTime);
            this.resultQueue.Add((init, end));
        }
        else
        {
            this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.UPDATE_DELIVERY, 1, MarkStatus.ABORT, "shipment"));
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

    // from down below, never invoked in default delivery worker implementation
    public void AddFinishedTransaction(TransactionOutput transactionOutput)
    {
        throw new NotImplementedException();
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        throw new NotImplementedException();
    }

    public List<TransactionOutput> GetFinishedTransactions()
    {
        throw new NotImplementedException();
    }

}

