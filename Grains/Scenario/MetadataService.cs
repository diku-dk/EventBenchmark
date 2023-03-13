using Common.Customer;
using GrainInterfaces.Scenario;
using Orleans;

namespace Grains.Scenario
{
    /**
     * Used as a way to decouple the master and the orchestrator grains
     */
    public class MetadataService : Grain, IMetadataService
    {
        private CustomerConfiguration customerConfiguration { get; set; }

        public void RegisterCustomerConfig(CustomerConfiguration customerConfiguration)
        {
            this.customerConfiguration = customerConfiguration;
        }

        public CustomerConfiguration RetriveCustomerConfig()
        {
            return customerConfiguration;
        }
    }
}
