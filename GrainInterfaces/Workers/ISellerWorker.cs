using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entities;
using Common.Scenario.Seller;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        public Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        [AlwaysInterleave]
        public Task<long> GetProductId();

    }
}
