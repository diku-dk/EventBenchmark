using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        public Task Init();
	}
}
