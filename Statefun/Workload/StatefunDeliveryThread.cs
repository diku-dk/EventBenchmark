using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using System.Text;
using Common.Workers;
using Common.Http;
using System.Collections.Concurrent;

namespace Statefun.Workers;

public class StatefunDeliveryThread : IDeliveryWorker
{
    string baseContentType = "application/vnd.marketplace/";

    protected readonly HttpClient httpClient;
    protected readonly DeliveryWorkerConfig config;

    protected readonly ILogger logger;

    protected readonly ConcurrentBag<TransactionMark> abortedTransactions;

    protected readonly ConcurrentBag<TransactionIdentifier> submittedTransactions;
    protected readonly ConcurrentBag<TransactionOutput> finishedTransactions;


    public static StatefunDeliveryThread BuildDeliveryThread(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new StatefunDeliveryThread(config, httpClientFactory.CreateClient(), logger);
    }

    private StatefunDeliveryThread(DeliveryWorkerConfig config, HttpClient httpClient, ILogger logger) 
    {       
        this.config = config;
        this.httpClient = httpClient;
        this.logger = logger;
        this.submittedTransactions = new();
        this.finishedTransactions = new();
        this.abortedTransactions = new(); 
    }    

	new public void Run(string tid)
	{

        string payLoad = "{ \"tid\" : " + tid + " }";

        string partitionID = tid;


        string apiUrl = string.Concat(this.config.shipmentUrl, "/", partitionID);        
        string eventType = "UpdateShipment";
        string contentType = string.Concat(baseContentType, eventType);

        var initTime = DateTime.UtcNow; 
        HttpResponseMessage resp = HttpUtils.SendHttpToStatefun(apiUrl, contentType, payLoad).Result;      

        if (resp.IsSuccessStatusCode)
        {
            TransactionIdentifier txId = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
            this.submittedTransactions.Add(txId);                        
        } else
        {
            this.abortedTransactions.Add( new TransactionMark(tid, TransactionType.UPDATE_DELIVERY, 1, MarkStatus.ABORT, "shipment")  );
            this.logger.LogDebug("Delivery worker failed to update delivery for TID {0}: {1}", tid, resp.ReasonPhrase);
        }
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

    public void AddFinishedTransaction(TransactionOutput transactionOutput)
    {
        this.finishedTransactions.Add(transactionOutput);
    }

    public List<TransactionOutput> GetFinishedTransactions()
    {
        var list = new List<TransactionOutput>();
        while (this.finishedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        var list = new List<TransactionIdentifier>();
        while (this.submittedTransactions.TryTake(out var item))
        {
            list.Add(item);
        }
        return list;
    }

    // never invoked in StatefunDeliveryThread
    public List<(TransactionIdentifier, TransactionOutput)> GetResults() {
        throw new NotImplementedException();
    }


}
