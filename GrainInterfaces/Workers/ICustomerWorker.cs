using System.Threading.Tasks;
using Common.Workload.Customer;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config);
    }
}
