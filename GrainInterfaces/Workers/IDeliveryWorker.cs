using Orleans;
using System.Threading.Tasks;
using Common.Workload.Delivery;

namespace GrainInterfaces.Workers
{
	public interface IDeliveryWorker : IGrainWithIntegerKey
    {
        public Task Init(DeliveryWorkerConfig config);
	}
}

