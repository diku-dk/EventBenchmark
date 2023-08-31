//using Common.Workload;
//using Common.Streaming;
//using Common.Workload.Seller;
//using Microsoft.Extensions.Logging;
//using Orleans.Streams;
//using Common.Distribution;

//namespace Grains.Workload;

//public sealed class ActorWorkloadManager : WorkloadManager
//{
//    private readonly IClusterClient orleansClient;
//    private readonly IStreamProvider streamProvider;

//    public ActorWorkloadManager(IClusterClient clusterClient,
//        IDictionary<TransactionType, int> transactionDistribution, 
//        DistributionType sellerDistribution, Interval sellerRange, 
//        DistributionType customerDistribution, Interval customerRange,
//        int concurrencyLevel, int executionTime, int delayBetweenRequests) : 
//        base(transactionDistribution, sellerDistribution, sellerRange, customerDistribution,
//            customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
//    {
//        this.orleansClient = clusterClient;
//        this.streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
//    }

//    protected override void SubmitTransaction(int tid, TransactionType type)
//    {
//        // this.logger.LogInformation("Sending a new {0} transaction with ID {1}", txId.type, txId.tid);
//        try
//        {
//            switch (type)
//            {
//                case TransactionType.CUSTOMER_SESSION:
//                {
//                    var grainID = this.keyGeneratorPerWorkloadType[type].Sample();
//                    // make sure there is no active session for the customer. if so, pick another customer
//                    int count = 1;
//                    while (!customerStatusCache.TryUpdate(grainID, WorkerStatus.ACTIVE, WorkerStatus.IDLE))
//                    {
//                        if (count > 5)
//                        {
//                            logger.LogWarning("[Workload emitter] Could not find an available customer! Perhaps should increase the number of customer next time?");
//                            while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
//                            return;
//                        }
//                        grainID = this.keyGeneratorPerWorkloadType[type].Sample();
//                        count++;
//                    }

//                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.CustomerWorkerNameSpace, grainID.ToString());
//                    streamOutgoing.OnNextAsync(tid).ContinueWith(x => this.customerStatusCache[grainID] = WorkerStatus.IDLE);
//                    break;
//                }
//                // delivery worker
//                case TransactionType.UPDATE_DELIVERY:
//                {
//                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConstants.DeliveryWorkerNameSpace, "0");
//                    streamOutgoing.OnNextAsync(tid).ContinueWith(x => Shared.ResultQueue.Writer.WriteAsync(ITEM));
//                    break;
//                }
//                // seller worker
//                case TransactionType.QUERY_DASHBOARD:
//                case TransactionType.PRICE_UPDATE:
//                case TransactionType.UPDATE_PRODUCT:
//                {
//                    var grainID = this.keyGeneratorPerWorkloadType[type].Sample();

//                    int count = 1;
//                    while (!sellerStatusCache.TryUpdate(grainID, WorkerStatus.ACTIVE, WorkerStatus.IDLE))
//                    {
//                        if (count > 5)
//                        {
//                            logger.LogDebug("[Workload emitter] A lot of sellers seem busy. Picking a busy one...");
//                            break;
//                        }
//                        grainID = this.keyGeneratorPerWorkloadType[type].Sample();
//                        count++;
//                    }

//                    var grainIDStr = grainID.ToString();
//                    logger.LogDebug("Seller worker {0} will be spawned!", grainIDStr);

//                    var streamOutgoing = this.streamProvider.GetStream<TransactionInput>(StreamingConstants.SellerWorkerNameSpace, grainIDStr);
//                    var txId = new TransactionInput(tid, type);
//                    if (type == TransactionType.QUERY_DASHBOARD)
//                    {
//                        streamOutgoing.OnNextAsync(txId).ContinueWith(x =>
//                        {
//                            this.sellerStatusCache[grainID] = WorkerStatus.IDLE;
//                            while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
//                        });
//                    }
//                    else
//                    {
//                        streamOutgoing.OnNextAsync(txId).ContinueWith(x => this.sellerStatusCache[grainID] = WorkerStatus.IDLE);
//                    }
//                    break;
//                }
//                default:
//                {
//                    long threadId = Environment.CurrentManagedThreadId;
//                    this.logger.LogError("Thread ID " + threadId + " Unknown transaction type defined!");
//                    break;
//                }
//            }
//        }
//        catch (Exception e)
//        {
//            long threadId = Environment.CurrentManagedThreadId;
//            this.logger.LogError("Thread ID {0} Error caught in SubmitTransaction: {1}", threadId, e.Message);
//        }

//    }


//}

