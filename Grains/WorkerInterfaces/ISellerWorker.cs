using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;

namespace Grains.WorkerInterfaces
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        Task<List<Latency>> Collect(DateTime finishTime);

        Task<long> GetProductId();

        Task RegisterFinishedTransaction(TransactionOutput output);

        Task<Product> GetProduct();
    }
}
