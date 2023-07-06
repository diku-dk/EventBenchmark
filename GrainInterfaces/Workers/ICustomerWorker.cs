using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entities;
using Common.Workload.Customer;
using Common.Workload.Metrics;
using Orleans;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{
        public Task Init(CustomerWorkerConfig config, Customer customer);

        public Task<List<Latency>> Collect(DateTime startTime);

        Task RegisterFinishedTransaction(TransactionOutput output);
    }
}
