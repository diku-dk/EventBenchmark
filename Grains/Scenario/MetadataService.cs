using Common.Customer;
using GrainInterfaces.Scenario;
using Orleans;

namespace Grains.Scenario
{
    public class MetadataService : Grain, IMetadataService
    {
        private CustomerConfiguration customerConfiguration { get; set; }

        public void WithCustomerConfig(CustomerConfiguration customerConfiguration)
        {
            this.customerConfiguration = customerConfiguration;
        }

        public CustomerConfiguration RetriveCustomerConfig()
        {
            return customerConfiguration;
        }
    }
}
