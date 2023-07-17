using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Orleans.Concurrency;

namespace Grains.WorkerInterfaces
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        Task<List<Latency>> Collect(DateTime startTime);

        Task<long> GetProductId();

        //[OneWay]
        Task RegisterFinishedTransaction(TransactionOutput output);

        Task<Product> GetProduct();
    }
}
