//using Common.Streaming;
//using Microsoft.Extensions.Logging;
//using Orleans.Streams;
//using Orleans.Concurrency;
//using Common.Workload.Metrics;
//using Common.Workload.Delivery;
//using Grains.WorkerInterfaces;

//namespace Grains.Workers
//{
//    [Reentrant]
//    public class DeliveryProxy : Grain, IDeliveryProxy
//    {
//        private DeliveryWorkerConfig config;
//        private IStreamProvider streamProvider;

//        private readonly ILogger<DeliveryProxy> logger;

//        private int actorId;

//        private readonly List<TransactionIdentifier> submittedTransactions;
//        private readonly List<TransactionOutput> finishedTransactions;

//        public DeliveryProxy(ILogger<DeliveryProxy> logger)
//        {
//            this.logger = logger;
//            this.submittedTransactions = new();
//            this.finishedTransactions = new();
//        }

//        public Task Init(DeliveryWorkerConfig config)
//        {
//            this.config = config;
//            this.submittedTransactions.Clear();
//            this.finishedTransactions.Clear();
//            return Task.CompletedTask;
//        }

//        public override async Task OnActivateAsync(CancellationToken cancellationToken)
//        {
//            this.actorId = (int) this.GetPrimaryKeyLong();
//            this.streamProvider = this.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
//            var workloadStream = streamProvider.GetStream<int>(StreamingConstants.DeliveryWorkerNameSpace, actorId.ToString());
//            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
//            if (subscriptionHandles_.Count > 0)
//            {
//                foreach (var subscriptionHandle in subscriptionHandles_)
//                {
//                    await subscriptionHandle.ResumeAsync(Run);
//                }
//            }
//            await workloadStream.SubscribeAsync<int>(Run);
//        }

//        // updating the delivery status of orders
//        public async Task Run(int tid, StreamSequenceToken token)
//        {
//            this.logger.LogInformation("Delivery {0}: Task started", this.actorId);
//            var worker = GrainFactory.GetGrain<IDeliveryWorker>(0);
//            var res = await worker.Send(tid, config.shipmentUrl + "/" + tid);
//            if (res.Item1)
//            {
//                this.submittedTransactions.Add(res.Item2);
//                this.finishedTransactions.Add(res.Item3);
//            }
//            this.logger.LogInformation("Delivery {0}: task terminated!", this.actorId);
//        }

//        public Task<(List<TransactionIdentifier>, List<TransactionOutput>)> Collect()
//        {
//            return Task.FromResult((submittedTransactions, finishedTransactions));
//        }

//    }
//}

