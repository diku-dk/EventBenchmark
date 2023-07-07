using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entities;
using Common.Workload.Metrics;
using Common.Workload.Seller;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{
	public interface ISellerWorker : IGrainWithIntegerKey
	{
        Task Init(SellerWorkerConfig sellerConfig, List<Product> products);

        Task<List<Latency>> Collect(DateTime startTime);

        [AlwaysInterleave]
        Task<long> GetProductId();

        Task RegisterFinishedTransaction(TransactionOutput output);

        [AlwaysInterleave]
        Task<Product> GetProduct();
    }
}
