//using Common.Collection;
//using Common.Entities;
//using Common.Experiment;
//using Common.Infra;
//using Common.Streaming;
//using Common.Streaming.Redis;
//using Common.Workflow;
//using Common.Workload;
//using Common.Workload.CustomerWorker;
//using Common.Workload.Delivery;
//using Common.Workload.Seller;
//using DuckDB.NET.Data;
//using Grains.Collection;
//using Grains.WorkerInterfaces;
//using Grains.Workload;
//using Microsoft.Extensions.Logging;
//using System.Text;

//namespace Orleans.Workload;

//public class ActorExperimentManager : ExperimentManager
//{
//    private IClusterClient orleansClient;

//    List<CancellationTokenSource> tokens = new(3);
//    List<Task> listeningTasks = new(3);

//    public ActorExperimentManager(ExperimentConfig config) : base(config)
//    {

//    }

//    protected override async void PreExperiment()
//    {
//        logger.LogInformation("Initializing Orleans client...");
//        this.orleansClient = await OrleansClientFactory.Connect();
//        if (orleansClient == null)
//        {
//            throw new Exception("Error on contacting Orleans Silo.");
//        }
//        var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
//        logger.LogInformation("Orleans client initialized!");

//        // if trash from last runs are active...
//        TrimStreams();

//        // eventual completion transactions
//        string redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);
//        foreach (var type in eventualCompletionTransactions)
//        {
//            if (config.transactionDistribution.ContainsKey(type))
//            {
//                var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
//                var token = new CancellationTokenSource();
//                tokens.Add(token);
//                listeningTasks.Add(SubscribeToRedisStream(redisConnection, channel, token));
//            }
//        }

//    }

//    // clean streams beforehand. make sure microservices do not receive events from previous runs
//    protected override async void TrimStreams()
//    {
//        string connection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);
//        logger.LogInformation("Triggering stream cleanup on {1}", connection);

//        List<string> channelsToTrimPlus = new();
//        channelsToTrimPlus.AddRange(config.streamingConfig.streams);

//        // should also iterate over all transaction mark streams and trim them
//        foreach (var type in eventualCompletionTransactions)
//        {
//            var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
//            channelsToTrimPlus.Add(channel);
//        }

//        logger.LogInformation("XTRIMming the following streams: {0}", channelsToTrimPlus);

//        await RedisUtils.TrimStreams(connection, channelsToTrimPlus);
//    }

//    public static async Task PrepareWorkers(IClusterClient orleansClient, IDictionary<TransactionType, int> transactionDistribution,
//          CustomerWorkerConfig customerWorkerConfig, SellerWorkerConfig sellerWorkerConfig, DeliveryWorkerConfig deliveryWorkerConfig,
//          List<Customer> customers, int numSellers, DuckDBConnection connection)
//    {
//        logger.LogInformation("Preparing workers...");
//        List<Task> tasks = new();

//        // defined dynamically
//        customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

//        // activate all customer workers
//        if (transactionDistribution.ContainsKey(TransactionType.CUSTOMER_SESSION))
//        {
//            var endValue = DuckDbUtils.Count(connection, "products");
//            if (endValue < customerWorkerConfig.maxNumberKeysToBrowse || endValue < customerWorkerConfig.maxNumberKeysToAddToCart)
//            {
//                throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
//            }

//            foreach (var customer in customers)
//            {
//                var customerWorker = orleansClient.GetGrain<ICustomerGrain>(customer.id);
//                tasks.Add(customerWorker.Init(customerWorkerConfig, customer));
//            }
//            await Task.WhenAll(tasks);
//            logger.LogInformation("{0} customers workers cleared!", customers.Count);
//            tasks.Clear();
//        }

//        // make sure to activate all sellers so they can respond to customers when required
//        // another solution is making them read from the microservice itself...
//        if (transactionDistribution.ContainsKey(TransactionType.PRICE_UPDATE) || transactionDistribution.ContainsKey(TransactionType.UPDATE_PRODUCT) || transactionDistribution.ContainsKey(TransactionType.QUERY_DASHBOARD))
//        {
//            for (int i = 1; i <= numSellers; i++)
//            {
//                List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
//                var sellerWorker = orleansClient.GetGrain<ISellerGrain>(i);
//                tasks.Add(sellerWorker.Init(sellerWorkerConfig, products));
//            }
//            await Task.WhenAll(tasks);
//            logger.LogInformation("{0} seller workers cleared!", numSellers);
//        }

//        // activate delivery worker
//        if (transactionDistribution.ContainsKey(TransactionType.UPDATE_DELIVERY))
//        {
//            var deliveryWorker = orleansClient.GetGrain<IDeliveryProxy>(0);
//            await deliveryWorker.Init(deliveryWorkerConfig);
//        }
//        logger.LogInformation("Workers prepared!");
//    }

//    protected override WorkloadManager SetUpManager(int runIdx)
//    {
//        return new ActorWorkloadManager(
//                this.orleansClient,
//                config.transactionDistribution,
//                config.runs[runIdx].sellerDistribution,
//                config.customerWorkerConfig.sellerRange,
//                config.runs[runIdx].customerDistribution,
//                customerRange,
//                config.concurrencyLevel,
//                config.executionTime,
//                config.delayBetweenRequests);
//    }

//    protected override async void Collect(int runIdx, DateTime startTime, DateTime finishTime)
//    {
//        var numSellers = (int)DuckDbUtils.Count(connection, "sellers");
//        MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, null);
//        await metricGather.Collect(startTime, finishTime, config.epoch, string.Format("#{0}_{1}_{2}_{3}_{4}", runIdx, config.concurrencyLevel, 
//            config.runs[runIdx].numProducts, config.runs[runIdx].sellerDistribution, config.runs[runIdx].keyDistribution));

//    }

//    protected override async void PostExperiment()
//    {
//        foreach (var token in tokens)
//        {
//            token.Cancel();
//        }

//        await Task.WhenAll(listeningTasks);

//        logger.LogInformation("Cleaning Redis streams....");
//        TrimStreams();
//    }

//    protected override async void PreWorkload(int runIdx)
//    {
//        logger.LogInformation("Waiting 10 seconds for initial convergence (stock <- product -> cart)");
//        var numSellers = (int)DuckDbUtils.Count(connection, "sellers");
//        await Task.WhenAll(
//            PrepareWorkers(orleansClient, config.transactionDistribution, config.customerWorkerConfig, config.sellerWorkerConfig, config.deliveryWorkerConfig, customers, numSellers, connection),
//            Task.Delay(TimeSpan.FromSeconds(10)));
//    }

//    private static Task SubscribeToRedisStream(string redisConnection, string channel, CancellationTokenSource token)
//    {
//        return Task.Factory.StartNew(async () =>
//        {
//            await RedisUtils.SubscribeStream(redisConnection, channel, token.Token, (entry) =>
//            {
//                while (!Shared.ResultQueue.Writer.TryWrite(ITEM)) { }
//                while (!Shared.FinishedTransactions.Writer.TryWrite(entry)) { }
//            });
//        }, TaskCreationOptions.LongRunning);

//    }

//}

