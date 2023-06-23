using System.Threading.Tasks;
using Common.Entities;
using Common.Workload.Customer;
using Orleans;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        Task Init(CustomerWorkerConfig config, Customer customer);
    }
}
