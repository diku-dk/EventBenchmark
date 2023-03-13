using Common.Customer;
using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Scenario
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        // Task<string> RetrieveAssignedQueue(int actorId);

        // allows decoupling scneario orchestrator with specific workers
        Task<CustomerConfiguration> RetriveCustomerConfig();

        void RegisterCustomerConfig(CustomerConfiguration customerConfiguration);

    }
}
