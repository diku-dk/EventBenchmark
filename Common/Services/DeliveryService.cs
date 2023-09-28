using Common.Workload.Metrics;
using Common.Workers;

namespace Common.Services;

public class DeliveryService : IDeliveryService
{
    private DeliveryThread deliveryThread;

    public DeliveryService(DeliveryThread deliveryThread)
	{
        this.deliveryThread = deliveryThread;
	}

    public void Run(string tid)
    {
        deliveryThread.Run(tid);
    }

    public List<(TransactionIdentifier, TransactionOutput)> GetResults()
    {
        return deliveryThread.GetResults();
    }
}


