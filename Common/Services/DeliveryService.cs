using Common.Workload.Metrics;
using Common.Workers;
using Common.Streaming;

namespace Common.Services;

public class DeliveryService : IDeliveryService
{
    private readonly DeliveryThread deliveryThread;

    public DeliveryService(DeliveryThread deliveryThread)
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

}


