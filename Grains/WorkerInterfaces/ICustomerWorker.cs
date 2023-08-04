using Common.Entities;
using Common.Workload.Customer;
using Common.Workload.Metrics;

namespace Grains.WorkerInterfaces
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config, Customer customer);

        Task<List<TransactionIdentifier>> Collect();

        Task Run(int tid);
    }
}
