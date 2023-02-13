using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{ 
		public Task Run();
	}
}
