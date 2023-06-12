using System.Threading.Tasks;
using Common.Scenario.Customer;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        public Task Init(CustomerWorkerConfig config);

    }
}
