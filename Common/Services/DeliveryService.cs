using Common.Workload.Metrics;
using Common.Streaming;
using Common.Workers.Delivery;
using Common.Workload.Delivery;

namespace Common.Services;

public class DeliveryService : IDeliveryService
{
    public delegate IDeliveryWorker BuildDeliveryWorkerDelegate(IHttpClientFactory httpClientFactory, DeliveryWorkerConfig config);

    private readonly IDeliveryWorker deliveryThread;

    public DeliveryService(IDeliveryWorker deliveryThread)
	{
        this.deliveryThread = deliveryThread;
	}

    public void Run(string tid)
    {
        this.deliveryThread.Run(tid);
    }

    public List<(TransactionIdentifier, TransactionOutput)> GetResults()
    {
        return this.deliveryThread.GetResults();
    }

    public List<TransactionMark> GetAbortedTransactions()
    {
        return this.deliveryThread.GetAbortedTransactions();
    }

    public void AddFinishedTransaction(TransactionOutput transactionOutput)
    {
        this.deliveryThread.AddFinishedTransaction(transactionOutput);
    }

    public List<TransactionIdentifier> GetSubmittedTransactions()
    {
        return this.deliveryThread.GetSubmittedTransactions();
    }

    public List<TransactionOutput> GetFinishedTransactions()
    {
        return this.deliveryThread.GetFinishedTransactions();
    }
}

