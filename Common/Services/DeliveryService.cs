using Common.Workload.Metrics;
using Common.Workers;
using Common.Streaming;

namespace Common.Services;

public class DeliveryService : IDeliveryService
{
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


