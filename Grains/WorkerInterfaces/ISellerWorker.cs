using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;

namespace Grains.WorkerInterfaces
{
	public interface ISellerGrain : IGrainWithIntegerKey
	{
        Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        Task<(List<TransactionIdentifier>, List<TransactionOutput>)> Collect();

        Task<int> GetProductId();

        Task<Product> GetProduct();
    }
}
