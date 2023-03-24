using System.Threading.Tasks;
using Orleans;
using Common.Scenario.Customer;
using Common.Scenario.Seller;

namespace GrainInterfaces.Scenario
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        // allows decoupling scenario orchestrator from workers

        // customer apis
        Task<CustomerConfiguration> RetrieveCustomerConfig();
        void RegisterCustomerConfig(CustomerConfiguration customerConfiguration);

        // seller apis
        Task<SellerConfiguration> RetrieveSellerConfig();
        void RegisterSellerConfig(SellerConfiguration sellerConfiguration);

    }
}
