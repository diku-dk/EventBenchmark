using Common.Workload.Metrics;

namespace Grains.WorkerInterfaces
{
	public interface IDeliveryWorker : IGrainWithIntegerKey
	{

		Task<(HttpResponseMessage, TransactionIdentifier, TransactionOutput)> Send(int tid, string url);

	}
}

