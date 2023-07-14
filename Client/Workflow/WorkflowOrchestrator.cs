﻿using Client.DataGeneration;
using Client.Infra;
using Client.Ingestion;
using Common.Http;
using Common.Workload;
using Common.Entities;
using Common.Streaming;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Client.Workload;
using Client.Ingestion.Config;
using Client.Streaming.Redis;
using Common.Infra;
using Client.Collection;
using Client.Cleaning;
using Common.Workload.Metrics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;
using Client.Experiment;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Common.Workload.Delivery;
using Grains.WorkerInterfaces;

namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        private static readonly ILogger logger = LoggerProxy.GetInstance("WorkflowOrchestrator");

        private static readonly List<TransactionType> transactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.DELETE_PRODUCT };

        public async static Task Run(ExperimentConfig config)
        {
            SyntheticDataSourceConfig previousData = new()
            {
                numProducts = 0
            };

            int runIdx = 0;
            int lastRunIdx = config.runs.Count() - 1;

            logger.LogInformation("Initializing Orleans client...");
            var orleansClient = await OrleansClientFactory.Connect();
            if (orleansClient == null)
            {
                logger.LogError("Error on contacting Orleans Silo.");
                return;
            }
            logger.LogInformation("Orleans client initialized!");

            using var connection = new DuckDBConnection(config.connectionString);
            connection.Open();

            var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            List<Customer> customers = null;
            long numSellers = 0;
            Interval customerRange = new Interval(1, config.numCustomers);

            List<CancellationTokenSource> tokens = new(3);
            List<Task> listeningTasks = new(3);

            // WorkloadGenerator workloadGen = new WorkloadGenerator(config.transactionDistribution, config.concurrencyLevel);

            // makes sure there are transactions
            // workloadGen.Prepare();

            // Task genTask = Task.Factory.StartNew(workloadGen.Run, TaskCreationOptions.LongRunning);

            foreach (var run in config.runs)
            {
                logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.UtcNow);

                // clean result queue
                //logger.LogInformation("Cleaning shared queues...");
                // while (Shared.ResultQueue.TryTake(out _));
                // clean to restart TID counting
                //while (Shared.Workload.TryTake(out _)) { }
                //logger.LogInformation("Finished cleaning shared queues.");

                // set the distributions accordingly
                config.customerWorkerConfig.sellerDistribution = run.sellerDistribution;
                config.sellerWorkerConfig.keyDistribution = run.keyDistribution;

                if (runIdx == 0 || run.numProducts != previousData.numProducts)
                {
                    previousData = new SyntheticDataSourceConfig()
                    {
                        connectionString = config.connectionString,
                        avgNumProdPerSeller = config.avgNumProdPerSeller,
                        numCustomers = config.numCustomers,
                        numProducts = run.numProducts
                    };
                    var syntheticDataGenerator = new SyntheticDataGenerator(previousData);

                    // dont need to generate customers on every run. only once
                    if (runIdx == 0)
                    {
                        // if trash from last runs are active...
                        await TrimStreams(config.streamingConfig.host, config.streamingConfig.port, config.streamingConfig.streams.ToList());

                        // cleanup databases
                        var resps_ = new List<Task<HttpResponseMessage>>();
                        foreach (var task in config.postExperimentTasks)
                        {
                            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                            logger.LogInformation("Pre experiment task to URL {0}", task.url);
                            resps_.Add(HttpUtils.client.SendAsync(message));
                        }
                        await Task.WhenAll(resps_);

                        // eventual completion transactions
                        string redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);

                        foreach (var type in transactions)
                        {
                            if (config.transactionDistribution.ContainsKey(type))
                            {
                                var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                                var token = new CancellationTokenSource();
                                tokens.Add(token);
                                listeningTasks.Add(SubscribeToTransactionResult(orleansClient, redisConnection, channel, token));
                            }
                        }

                        syntheticDataGenerator.Generate(connection, true);
                        // customers are fixed accross runs
                        customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                    }
                    else
                    {
                        syntheticDataGenerator.Generate(connection, false);
                    }

                    var ingestionOrchestrator = new IngestionOrchestrator(config.ingestionConfig);
                    await ingestionOrchestrator.Run(connection);

                    // get number of sellers
                    numSellers = DuckDbUtils.Count(connection, "sellers");
                }

                await PrepareWorkers(orleansClient, config.transactionDistribution, config.customerWorkerConfig, config.sellerWorkerConfig,
                    config.deliveryWorkerConfig, customers, numSellers, connection);

                WorkloadEmitter emitter = new WorkloadEmitter(
                    orleansClient,
                    config.transactionDistribution,
                    run.sellerDistribution,
                    config.customerWorkerConfig.sellerRange,
                    run.customerDistribution,
                    customerRange,
                    config.concurrencyLevel,
                    config.executionTime,
                    config.delayBetweenRequests);

                var emitTask = await emitter.Run();

                DateTime startTime = emitTask.startTime;
                DateTime finishTime = emitTask.finishTime;

                logger.LogInformation("Waiting 10 seconds for results to arrive from Redis...");
                await Task.Delay(TimeSpan.FromSeconds(10));

                // set up data collection for metrics
                // TODO send the distributions and the config (customers,sellers, etc) so it can be written to the file
                MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, null);
                await metricGather.Collect(startTime, finishTime, runIdx.ToString());

                // trim first to avoid receiving events after the post run task
                await TrimStreams(config.streamingConfig.host, config.streamingConfig.port, config.streamingConfig.streams.ToList());

                // reset data in microservices - post run
                if (runIdx < lastRunIdx)
                {
                    logger.LogInformation("Post run tasks started");
                    var responses = new List<Task<HttpResponseMessage>>();
                    // must call the cleanup if next run changes number of products
                    if (runIdx < (lastRunIdx - 1) && config.runs[runIdx + 1].numProducts != run.numProducts)
                    {
                        logger.LogInformation("Next run changes the number of products.");
                        foreach (var task in config.postExperimentTasks)
                        {
                            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                            logger.LogInformation("Post run task to URL {0}", task.url);
                            responses.Add(HttpUtils.client.SendAsync(message));
                        }
                    }
                    else
                    {
                        logger.LogInformation("Next run does not change the number of products.");
                        foreach (var task in config.postRunTasks)
                        {
                            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                            logger.LogInformation("Post run task to URL {0}", task.url);
                            responses.Add(HttpUtils.client.SendAsync(message));
                        }
                    }
                    await Task.WhenAll(responses);
                    logger.LogInformation("Post run tasks finished");
                }

                logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);
                runIdx++;

                if (runIdx < lastRunIdx)
                {
                    logger.LogInformation("Starting new run in {0} seconds...", config.delayBetweenRuns / 1000);
                    await Task.Delay(config.delayBetweenRuns);
                }
            }

            //workloadGen.Stop();
            // to make sure the generator leaves the loop
            //Shared.WaitHandle.Add(0);

            foreach (var token in tokens)
            {
                token.Cancel();
            }

            await Task.WhenAll(listeningTasks);

            // await orleansClient.();
            // logger.LogInformation("Orleans client finalized!");

            logger.LogInformation("Post experiment cleanup tasks started.");
            var resps = new List<Task<HttpResponseMessage>>();
            foreach (var task in config.postExperimentTasks)
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                logger.LogInformation("Post experiment task to URL {0}", task.url);
                resps.Add(HttpUtils.client.SendAsync(message));
            }
            await Task.WhenAll(resps);
            logger.LogInformation("Post experiment cleanup tasks finished");

            logger.LogInformation("Experiment finished");
        }

        /**
         * Single run
         */
        public async static Task Run(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig,
            WorkloadConfig workloadConfig, CollectionConfig collectionConfig, CleaningConfig cleaningConfig)
        {

            if (workflowConfig.healthCheck)
            {
                var responses = new List<Task<HttpResponseMessage>>();
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    var urlHealth = tableUrl.Value + WorkflowConfig.healthCheckEndpoint;
                    logger.LogInformation("Contacting {0} healthcheck on {1}", tableUrl.Key, urlHealth);
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Get, urlHealth);
                    responses.Add(HttpUtils.client.SendAsync(message));
                }

                try
                {
                    await Task.WhenAll(responses);
                } catch(Exception e)
                {
                    logger.LogError("Error on contacting healthcheck: {0}", e.Message);
                    return;
                }

                int idx = 0;
                foreach (var tableUrl in ingestionConfig.mapTableToUrl)
                {
                    if (!responses[idx].Result.IsSuccessStatusCode)
                    {
                        logger.LogError("Healthcheck failed for {0} in URL {1}", tableUrl.Key, tableUrl.Value);
                        return;
                    }
                    idx++;
                }

                logger.LogInformation("Healthcheck succeeded for all URLs {1}", ingestionConfig.mapTableToUrl);
                string redisConn = string.Format("{0}:{1}", workloadConfig.streamingConfig.host, workloadConfig.streamingConfig.port);
                // https://stackoverflow.com/questions/27102351/how-do-you-handle-failed-redis-connections
                // https://stackoverflow.com/questions/47348341/servicestack-redis-service-availability
                if (workflowConfig.transactionSubmission && !RedisUtils.TestRedisConnection(redisConn))
                {
                    logger.LogInformation("Healthcheck failed for Redis in URL {0}", redisConn);
                }

                logger.LogInformation("Healthcheck process succeeded");
            }

            if (workflowConfig.dataLoad)
            {
                using (DuckDBConnection connection = new DuckDBConnection(syntheticDataConfig.connectionString))
                {
                    var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                    connection.Open();
                    syntheticDataGenerator.Generate(connection);
                }
            }

            if (workflowConfig.ingestion)
            {
                using (DuckDBConnection connection = new DuckDBConnection(ingestionConfig.connectionString))
                {
                    var ingestionOrchestrator = new IngestionOrchestrator(ingestionConfig);
                    connection.Open();
                    await ingestionOrchestrator.Run(connection);
                }
            }

            if (workflowConfig.transactionSubmission)
            {

                logger.LogInformation("Initializing Orleans client...");
                var orleansClient = await OrleansClientFactory.Connect();
                if (orleansClient == null) {
                    logger.LogError("Error on contacting Orleans Silo.");
                    return;
                }
                logger.LogInformation("Orleans client initialized!");

                var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);

                // get number of sellers
                using var connection = new DuckDBConnection(workloadConfig.connectionString);
                connection.Open();
                List<Customer> customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                long numSellers = DuckDbUtils.Count(connection, "sellers");
                var customerRange = new Interval(1, customers.Count());

                // initialize all workers
                await PrepareWorkers(orleansClient, workloadConfig.transactionDistribution, workloadConfig.customerWorkerConfig, workloadConfig.sellerWorkerConfig,
                    workloadConfig.deliveryWorkerConfig, customers, numSellers, connection);

                // eventual completion transactions
                string redisConnection = string.Format("{0}:{1}", workloadConfig.streamingConfig.host, workloadConfig.streamingConfig.port);

                List<CancellationTokenSource> tokens = new(3);
                List<Task> listeningTasks = new(3);
                foreach (var type in transactions)
                {
                    var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                    var token = new CancellationTokenSource();
                    tokens.Add(token);
                    listeningTasks.Add(SubscribeToTransactionResult(orleansClient, redisConnection, channel, token));
                }

                //WorkloadGenerator workloadGen = new WorkloadGenerator(workloadConfig.transactionDistribution, workloadConfig.concurrencyLevel);

                //// makes sure there are transactions
                //workloadGen.Prepare();

                //Task genTask = Task.Factory.StartNew(workloadGen.Run, TaskCreationOptions.LongRunning);

                // setup transaction orchestrator
                WorkloadEmitter emitter = new WorkloadEmitter( orleansClient, workloadConfig.transactionDistribution, workloadConfig.customerWorkerConfig.sellerDistribution,
                    workloadConfig.customerWorkerConfig.sellerRange, workloadConfig.customerDistribution, customerRange,
                    workloadConfig.concurrencyLevel, workloadConfig.executionTime, workloadConfig.delayBetweenRequests);

                Task<(DateTime startTime, DateTime finishTime)> emitTask = Task.Run(emitter.Run);

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(emitTask, OrleansClientFactory._siloFailedTask.Task);

                DateTime startTime = emitTask.Result.startTime;
                DateTime finishTime = emitTask.Result.finishTime;

                foreach (var token in tokens)
                {
                    token.Cancel();
                }

                await Task.WhenAll(listeningTasks);

                // set up data collection for metrics
                if (workflowConfig.collection)
                {
                    MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, collectionConfig);
                    await metricGather.Collect(startTime, finishTime);
                }

                //workloadGen.Stop();
                //// to make sure the generator leaves the loop
                //Shared.WaitHandle.Add(0);

                // await orleansClient.Close();
                // logger.LogInformation("Orleans client finalized!");
            }

            if (workflowConfig.cleanup)
            {
                await Clean(cleaningConfig);
            }

            return;

        }

        private static async Task TrimStreams(string host, int port, List<string> channelsToTrim)
        {
            // clean streams beforehand. make sure microservices do not receive events from previous runs
            
            string connection = string.Format("{0}:{1}", host, port);
            logger.LogInformation("Triggering stream cleanup on {1}", connection);

            // should also iterate over all transaction mark streams and trim them
            foreach (var type in transactions)
            {
                var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                channelsToTrim.Add(channel);
            }

            Task trimTasks = Task.Run(() => RedisUtils.TrimStreams(connection, channelsToTrim));
            await trimTasks;
        }

        private static async Task Clean(CleaningConfig cleaningConfig)
        {
            List<string> channelsToTrim = cleaningConfig.streamingConfig.streams.ToList();
            await TrimStreams(cleaningConfig.streamingConfig.host, cleaningConfig.streamingConfig.port, channelsToTrim);

            List<Task> responses = new();
            // truncate duckdb tables
            foreach (var entry in cleaningConfig.mapMicroserviceToUrl)
            {
                var urlCleanup = entry.Value + CleaningConfig.cleanupEndpoint;
                logger.LogInformation("Triggering {0} cleanup on {1}", entry.Key, urlCleanup);
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, urlCleanup);
                responses.Add(HttpUtils.client.SendAsync(message));
            }
            await Task.WhenAll(responses);
            // DuckDbUtils.DeleteAll(connection, "products", "seller_id = " + i);
        }

        public static async Task PrepareWorkers(IClusterClient orleansClient, IDictionary<TransactionType,int> transactionDistribution,
             CustomerWorkerConfig customerWorkerConfig, SellerWorkerConfig sellerWorkerConfig, DeliveryWorkerConfig deliveryWorkerConfig,
             List<Customer> customers, long numSellers, DuckDBConnection connection)
        {
            logger.LogInformation("Preparing workers...");
            List<Task> tasks = new();

            // defined dynamically
            customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

            // activate all customer workers
            if (transactionDistribution.ContainsKey(TransactionType.CUSTOMER_SESSION))
            {
                var endValue = DuckDbUtils.Count(connection, "products");
                if (endValue < customerWorkerConfig.maxNumberKeysToBrowse || endValue < customerWorkerConfig.maxNumberKeysToAddToCart)
                {
                    throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
                }

                foreach (var customer in customers)
                {
                    var customerWorker = orleansClient.GetGrain<ICustomerWorker>(customer.id);
                    tasks.Add(customerWorker.Init(customerWorkerConfig, customer));
                }
                await Task.WhenAll(tasks);
            }

            // make sure to activate all sellers so they can respond to customers when required
            // another solution is making them read from the microservice itself...
            if (transactionDistribution.ContainsKey(TransactionType.PRICE_UPDATE) || transactionDistribution.ContainsKey(TransactionType.DELETE_PRODUCT) || transactionDistribution.ContainsKey(TransactionType.DASHBOARD))
            {
                tasks.Clear();
                for (int i = 1; i <= numSellers; i++)
                {
                    List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                    var sellerWorker = orleansClient.GetGrain<ISellerWorker>(i);
                    tasks.Add(sellerWorker.Init(sellerWorkerConfig, products));
                }
                await Task.WhenAll(tasks);
            }

            // activate delivery worker
            if (transactionDistribution.ContainsKey(TransactionType.UPDATE_DELIVERY))
            {
                var deliveryWorker = orleansClient.GetGrain<IDeliveryWorker>(0);
                await deliveryWorker.Init(deliveryWorkerConfig);
            }
            logger.LogInformation("Workers prepared!");
        }

        private static Task SubscribeToTransactionResult(IClusterClient orleansClient, string redisConnection, string channel, CancellationTokenSource token)
        {
            return Task.Factory.StartNew(() => RedisUtils.Subscribe(redisConnection, channel, token.Token, entries =>
            {
                var now = DateTime.UtcNow;
                foreach (var entry in entries)
                {
                    try
                    {
                        // Dapr event payload deserialization
                        JObject d = JsonConvert.DeserializeObject<JObject>(entry.Values[0].Value.ToString());
                        TransactionMark mark = JsonConvert.DeserializeObject<TransactionMark>(d.SelectToken("['data']").ToString());

                        if (mark.type == TransactionType.CUSTOMER_SESSION)
                        {
                            orleansClient.GetGrain<ICustomerWorker>(mark.actorId).RegisterFinishedTransaction(new TransactionOutput(mark.tid, now));
                        }
                        else
                        {
                            orleansClient.GetGrain<ISellerWorker>(mark.actorId).RegisterFinishedTransaction(new TransactionOutput(mark.tid, now));
                        }
                        // logger.LogInformation("Processed the transaction mark {0} | {1} at {2}", mark.tid, mark.type, now);
                    }
                    catch (Exception e)
                    {
                        logger.LogWarning("Error processing transaction mark event: {0}", e.Message);
                    }
                    //finally
                    //{
                    //    // let emitter aware this request has finished
                    //    Shared.ResultQueue.Add(0);
                    //}
                }
            }), TaskCreationOptions.LongRunning);
            
        }

    }
}