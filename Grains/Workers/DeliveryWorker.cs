//using Common.Workload;
//using Common.Workload.Metrics;
//using Grains.WorkerInterfaces;
//using Orleans.Concurrency;

//namespace Grains.Workers
//{
//    [StatelessWorker]
//    public class DeliveryWorker : Grain, IDeliveryWorker
//	{
//        private readonly HttpClient httpClient;

//        public DeliveryWorker(HttpClient httpClient)
//        {
//            this.httpClient = httpClient;
//        }

//        public Task<(bool IsSuccessStatusCode, TransactionIdentifier, TransactionOutput)> Send(int tid, string url)
//        {
//            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, url);
//            var initTime = DateTime.UtcNow;
//            var init = new TransactionIdentifier(tid, TransactionType.UPDATE_DELIVERY, initTime);
//            var resp = httpClient.Send(message);
//            var endTime = DateTime.UtcNow;
//            var end = new TransactionOutput(tid, endTime);
//            return Task.FromResult((resp.IsSuccessStatusCode, init, end));
//        }
//    }
//}

