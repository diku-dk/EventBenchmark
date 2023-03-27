using System.Threading.Tasks;
using Common.Scenario.Seller;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        public Task Init(SellerConfiguration sellerConfig);

        // get a product ID respecting the key distribution of the seller
        [AlwaysInterleave]
        public Task<long> GetProductId();

	}
}
