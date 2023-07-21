using Common.Workload.Metrics;

namespace Grains.WorkerInterfaces
{
	public interface IDeliveryWorker : IGrainWithIntegerKey
	{

		Task<(bool IsSuccessStatusCode, TransactionIdentifier, TransactionOutput)> Send(int tid, string url);

	}
}

