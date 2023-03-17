using System.Threading.Tasks;
using Orleans;
using Common.Scenario.Customer;

namespace GrainInterfaces.Scenario
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        // Task<string> RetrieveAssignedQueue(int actorId);

        // allows decoupling scneario orchestrator with specific workers
        Task<CustomerConfiguration> RetrieveCustomerConfig();

        void RegisterCustomerConfig(CustomerConfiguration customerConfiguration);

    }
}
