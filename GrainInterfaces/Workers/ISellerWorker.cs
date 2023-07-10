using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Orleans;

namespace GrainInterfaces.Workers
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        Task<List<Latency>> Collect(DateTime startTime);

        Task<long> GetProductId();

        Task RegisterFinishedTransaction(TransactionOutput output);

        Task<Product> GetProduct();
    }
}
