using Common.Customer;
using Orleans;

namespace GrainInterfaces.Scenario
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        // Task<string> RetrieveAssignedQueue(int actorId);

        // allows decoupling scneario orchestrator with specific workers
        CustomerConfiguration RetriveCustomerConfig();

        void RegisterCustomerConfig(CustomerConfiguration customerConfiguration);

    }
}
