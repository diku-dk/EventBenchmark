using System.Threading.Tasks;
using Common.Customer;
using Orleans;

namespace GrainInterfaces.Workers
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        Task<string> RetrieveAssignedQueue(int actorId);

        // allows decoupling scneario orchestrator with specific workers
        CustomerConfiguration GetCustomerConfiguration();

    }
}
