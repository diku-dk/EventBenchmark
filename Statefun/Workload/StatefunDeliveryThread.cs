using Common.Infra;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Delivery;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using System.Collections.Concurrent;
using Statefun.Infra;
using Common.Workers.Delivery;

namespace Statefun.Workers;

public sealed class StatefunDeliveryThread : IDeliveryWorker
{

    private readonly DeliveryWorkerConfig config;

    private readonly ILogger logger;

    private readonly ConcurrentBag<TransactionMark> abortedTransactions;

    private readonly ConcurrentBag<TransactionIdentifier> submittedTransactions;

    private readonly ConcurrentBag<TransactionOutput> finishedTransactions;

    public static StatefunDeliveryThread BuildDeliveryThread(DeliveryWorkerConfig config)
    {
        var logger = LoggerProxy.GetInstance("Delivery");
        return new StatefunDeliveryThread(config, logger);
    }

    private StatefunDeliveryThread(DeliveryWorkerConfig config, ILogger logger) 
    {       
        this.config = config;
        this.logger = logger;
        this.submittedTransactions = new();
        this.finishedTransactions = new();
        this.abortedTransactions = new(); 
    }    

	public void Run(string tid)
	{

        string payLoad = "{ \"tid\" : " + tid + " }";

        string partitionID = tid;


        string apiUrl = string.Concat(this.config.shipmentUrl, "/", partitionID);        
        string eventType = "UpdateShipment";
        string contentType = string.Concat(StatefunUtils.BASE_CONTENT_TYPE, eventType);

        var initTime = DateTime.UtcNow; 
        HttpResponseMessage resp = StatefunUtils.SendHttpToStatefun(apiUrl, contentType, payLoad).Result;      

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
