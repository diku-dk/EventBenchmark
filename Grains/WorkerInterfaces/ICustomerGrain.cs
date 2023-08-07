using Common.Entities;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;

namespace Grains.WorkerInterfaces
{
	public interface ICustomerGrain : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config, Customer customer);

        Task<List<TransactionIdentifier>> Collect();

        Task Run(int tid);
    }
}
