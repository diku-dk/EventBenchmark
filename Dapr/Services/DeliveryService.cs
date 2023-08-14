using Common.Workload.Metrics;
using Daprr.Workers;

namespace Daprr.Services;

public class DeliveryService : IDeliveryService
{
    private DeliveryThread deliveryThread;

    public DeliveryService(DeliveryThread deliveryThread)
	{
        this.deliveryThread = deliveryThread;
	}

    public void Run(int tid)
    {
        deliveryThread.Run(tid);
    }

    public List<(TransactionIdentifier, TransactionOutput)> GetResults()
    {
        return deliveryThread.GetResults();
    }
}


