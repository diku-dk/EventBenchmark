using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Client.DataGeneration;
using Client.Infra;
using Client.Ingestion;
using Common.Http;
using Common.Workload;
using Common.Entities;
using Common.Streaming;
using DuckDB.NET.Data;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using Orleans;
using Client.Workload;
using Client.Ingestion.Config;
using Client.Streaming.Redis;
using Common.Infra;
using Client.Collection;
using System.Linq;
using Client.Cleaning;
using Common.Workload.Metrics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Threading;
using Client.Experiment;
using Common.Workload.Customer;
using Common.Workload.Seller;
using Common.Workload.Delivery;

namespace Client.Workflow
{
	public class WorkflowOrchestrator
	{

        private static readonly ILogger logger = LoggerProxy.GetInstance("WorkflowOrchestrator");

        private static readonly List<TransactionType> transactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.DELETE_PRODUCT };

        public async static Task Run(ExperimentConfig config)
        {
            SyntheticDataSourceConfig previousData = null;
            int runIdx = 0;

            logger.LogInformation("Initializing Orleans client...");
            var orleansClient = await OrleansClientFactory.Connect();
            if (orleansClient == null)
            {
                logger.LogError("Error on contacting Orleans Silo.");
                return;
            }
            logger.LogInformation("Orleans client initialized!");

            using var connection = new DuckDBConnection(config.connectionString);

            var streamProvider = orleansClient.GetStreamProvider(StreamingConstants.DefaultStreamProvider);
            List<Customer> customers = null;
            long numSellers = 0;
            Interval customerRange = new Interval(1, config.numCustomers);

            List<CancellationTokenSource> tokens = new(3);
            List<Task> listeningTasks = new(3);


            foreach (var run in config.runs)
            {
                logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.Now);

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

                        // eventual completion transactions
                        string redisConnection = string.Format("{0}:{1}", config.streamingConfig.host, config.streamingConfig.port);

                        foreach (var type in transactions)
                        {
                            var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                            var token = new CancellationTokenSource();
                            tokens.Add(token);
                            listeningTasks.Add(SubscribeToTransactionResult(orleansClient, redisConnection, channel, token));
                        }

                        syntheticDataGenerator.Generate(true);
                    }
                    else
                    {
                        syntheticDataGenerator.Generate(false);
                    }

                    config.ingestionConfig.connectionString = config.connectionString;
                    var ingestionOrchestrator = new IngestionOrchestrator(config.ingestionConfig);
                    await ingestionOrchestrator.Run();

                    // get number of sellers
                    connection.Open();
                    customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
                    numSellers = DuckDbUtils.Count(connection, "sellers");

                }

                await PrepareWorkers(config.customerWorkerConfig, config.sellerWorkerConfig,
                    config.deliveryWorkerConfig, orleansClient, customers, numSellers, connection);

                //List<CancellationTokenSource> tokens = new(3);
                //List<Task> listeningTasks = new(3);
                //foreach (var type in transactions)
                //{
                //    var channel = new StringBuilder(nameof(TransactionMark)).Append('_').Append(type.ToString()).ToString();
                //    var token = new CancellationTokenSource();
                //    tokens.Add(token);
                //    listeningTasks.Add(SubscribeToTransactionResult(orleansClient, redisConnection, channel, token));
                //}

                var workloadTask = await Task.Run(() => WorkloadOrchestrator.Run(
                            orleansClient, config.transactionDistribution, config.concurrencyLevel, config.customerWorkerConfig.sellerDistribution,
                            config.customerWorkerConfig.sellerRange, run.customerDistribution, customerRange, config.delayBetweenRequests, config.executionTime));
                DateTime startTime = workloadTask.startTime;
                DateTime finishTime = workloadTask.finishTime;

                //foreach (var token in tokens)
                //{
                //    token.Cancel();
                //}

                //await Task.WhenAll(listeningTasks);

                // set up data collection for metrics
                // TODO send the distributions and the config (customers,sellers, etc) so it can be written to the file
                MetricGather metricGather = new MetricGather(orleansClient, customers, numSellers, null);
                await metricGather.Collect(startTime, finishTime);

                // trim first to avoid receiving events after the post run task
                await TrimStreams(config.streamingConfig.host, config.streamingConfig.port, config.streamingConfig.streams.ToList());

                // reset data in microservices - post run
                var responses = new List<Task<HttpResponseMessage>>();
                logger.LogInformation("Post run tasks started");
                foreach (var task in config.postRunTasks)
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                    logger.LogInformation("Post run task to URL {0}", task.url);
                    responses.Add(HttpUtils.client.SendAsync(message));
                }
                logger.LogInformation("Post run tasks finished");

                logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.Now);
                runIdx++;

            }

            foreach (var token in tokens)
            {
                token.Cancel();
            }

            await Task.WhenAll(listeningTasks);

            await orleansClient.Close();
            logger.LogInformation("Orleans client finalized!");

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
                var syntheticDataGenerator = new SyntheticDataGenerator(syntheticDataConfig);
                syntheticDataGenerator.Generate();
            }

            if (workflowConfig.ingestion)
            {
                var ingestionOrchestrator = new IngestionOrchestrator(ingestionConfig);
                await ingestionOrchestrator.Run();
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
                await PrepareWorkers(workloadConfig.customerWorkerConfig, workloadConfig.sellerWorkerConfig,
                    workloadConfig.deliveryWorkerConfig, orleansClient, customers, numSellers, connection);

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

                // setup transaction orchestrator
                var workloadTask = Task.Run( ()=> WorkloadOrchestrator.Run( orleansClient, workloadConfig.transactionDistribution, workloadConfig.concurrencyLevel, workloadConfig.customerWorkerConfig.sellerDistribution,
                    workloadConfig.customerWorkerConfig.sellerRange, workloadConfig.customerDistribution, customerRange, workloadConfig.delayBetweenRequests, workloadConfig.executionTime));
                DateTime startTime = workloadTask.Result.startTime;
                DateTime finishTime = workloadTask.Result.finishTime;

                // listen for cluster client disconnect and stop the sleep if necessary... Task.WhenAny...
                await Task.WhenAny(workloadTask, OrleansClientFactory._siloFailedTask.Task);

                foreach(var token in tokens)
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

                await orleansClient.Close();
                logger.LogInformation("Orleans client finalized!");
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

        public static async Task PrepareWorkers(CustomerWorkerConfig customerWorkerConfig, SellerWorkerConfig sellerWorkerConfig, DeliveryWorkerConfig deliveryWorkerConfig,
            IClusterClient orleansClient, List<Customer> customers, long numSellers, DuckDBConnection connection)
        {
            logger.LogInformation("Preparing workers...");

            var endValue = DuckDbUtils.Count(connection, "products");
            if (endValue < customerWorkerConfig.maxNumberKeysToBrowse || endValue < customerWorkerConfig.maxNumberKeysToAddToCart)
            {
                throw new Exception("Number of available products < possible number of keys to checkout. That may lead to customer grain looping forever!");
            }

            // defined dynamically
            customerWorkerConfig.sellerRange = new Interval(1, (int)numSellers);

            // activate all customer workers
            List<Task> tasks = new();

            ICustomerWorker customerWorker = null;
            foreach (var customer in customers)
            {
                customerWorker = orleansClient.GetGrain<ICustomerWorker>(customer.id);
                tasks.Add(customerWorker.Init(customerWorkerConfig, customer));
            }
            await Task.WhenAll(tasks);

            // make sure to activate all sellers so they can respond to customers when required
            // another solution is making them read from the microservice itself...
            ISellerWorker sellerWorker = null;
            tasks.Clear();
            for (int i = 1; i <= numSellers; i++)
            {
                List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
                sellerWorker = orleansClient.GetGrain<ISellerWorker>(i);
                tasks.Add(sellerWorker.Init(sellerWorkerConfig, products));
            }
            await Task.WhenAll(tasks);

            // activate delivery worker
            var deliveryWorker = orleansClient.GetGrain<IDeliveryWorker>(0);
            await deliveryWorker.Init(deliveryWorkerConfig);

            logger.LogInformation("Workers prepared!");
        }

        private static Task SubscribeToTransactionResult(IClusterClient orleansClient, string redisConnection, string channel, CancellationTokenSource token)
        {
            return Task.Run(() => RedisUtils.Subscribe(redisConnection, channel, token.Token, entries =>
            {
                var now = DateTime.Now;
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
                        logger.LogInformation("Processed the transaction mark {0} | {1} at {2}", mark.tid, mark.type, now);
                    }
                    catch (Exception e)
                    {
                        logger.LogWarning("Error processing transaction mark event: {0}", e.Message);
                    }
                    finally
                    {
                        // let emitter aware this request has finished
                        Shared.ResultQueue.Add(0);
                    }
                }
            }));
            
        }

    }
}