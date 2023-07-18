using Common.Http;
using Common.Workload;
using Common.Workload.Metrics;
using Grains.WorkerInterfaces;
using Orleans.Concurrency;

namespace Grains.Workers
{
    [StatelessWorker]
    public class DeliveryWorker : Grain, IDeliveryWorker
	{

        public Task<(HttpResponseMessage, TransactionIdentifier, TransactionOutput)> Send(int tid, string url)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, url);
            var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, DateTime.UtcNow);
            var resp = HttpUtils.client.Send(message);
            var end = new TransactionOutput(tid, DateTime.UtcNow);
            return Task.FromResult((resp, init, end));
        }
    }
}

