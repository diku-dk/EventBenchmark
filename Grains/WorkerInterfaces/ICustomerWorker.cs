using Common.Entities;
using Common.Workload.Customer;
using Common.Workload.Metrics;
using Orleans.Concurrency;

namespace Grains.WorkerInterfaces
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config, Customer customer);

        Task<List<Latency>> Collect(DateTime finishTime);

        [OneWay]
        Task RegisterFinishedTransaction(TransactionOutput output);
    }
}
