using GrainInterfaces.Scenario;
using System.Threading.Tasks;
using Orleans;
using Common.Scenario.Customer;

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

        public Task<CustomerConfiguration> RetrieveCustomerConfig()
        {
            return Task.FromResult( customerConfiguration );
        }
    }
}
