using System.Threading.Tasks;
using Common.Scenario.Customer;
using Common.Entity;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        public Task Init(CustomerConfiguration config);

    }
}
