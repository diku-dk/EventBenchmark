using Common.Entities;
using Common.Http;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Dynamic;
using Common.Infra;
using Common.Workload;
using System.Text;
using Grains.WorkerInterfaces;
using Client.Workload;
using Common.Streaming;
using static Client.Streaming.Redis.RedisUtils;

namespace Client.Collection
{
    public class MetricGather
    {
        private readonly IClusterClient orleansClient;
        private readonly List<Customer> customers;
        private readonly long numSellers;
        private readonly CollectionConfig collectionConfig;
        private static readonly ILogger logger = LoggerProxy.GetInstance("MetricGather");

        public MetricGather(IClusterClient orleansClient, List<Customer> customers, long numSellers, CollectionConfig collectionConfig)
        {
            this.orleansClient = orleansClient;
            this.customers = customers;
            this.numSellers = numSellers;
            this.collectionConfig = collectionConfig;
        }

        private List<Latency> BuildLatencyList(Dictionary<long, TransactionIdentifier> submitted, Dictionary<long, TransactionOutput> finished, DateTime finishTime, string workerType = "")
        {
            var targetValues = finished.Values.Where(e => e.timestamp.CompareTo(finishTime) <= 0);
            var latencyList = new List<Latency>(targetValues.Count());
            foreach (var entry in targetValues)
            {
                if (!submitted.ContainsKey(entry.tid))
                {
                    logger.LogWarning("[{0}] Cannot find correspondent submitted TID from finished transaction {0}", workerType, entry);
                    continue;
                }
                var init = submitted[entry.tid];
                var latency = (entry.timestamp - init.timestamp).TotalMilliseconds;
                if (latency < 0)
                {
                    logger.LogWarning("[{0}] Negative latency found for TID {1}. Init {2} End {3}", workerType, entry.tid, init, entry);
                    continue;
                }
                latencyList.Add(new Latency(entry.tid, init.type, latency, entry.timestamp));

            }
            return latencyList;
        }

        private async Task<List<Latency>> CollectFromSeller(List<Entry> entries, DateTime finishTime)
        {
            int dupSub = 0;
            int dupFin = 0;
            List<Task<(List<TransactionIdentifier>, List<TransactionOutput>)>> taskList = new();
            for (int i = 1; i <= numSellers; i++)
            {
                var sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                taskList.Add(sellerWorker.Collect());
            }
            await Task.WhenAll(taskList);

            Dictionary<long, TransactionIdentifier> sellerSubmitted = new();
            Dictionary<long, TransactionOutput> sellerFinished = new();

            foreach (var task in taskList)
            {
                var res = task.Result;
                foreach (var submitted in res.Item1)
                {
                    if (!sellerSubmitted.TryAdd(submitted.tid, submitted))
                    {
                        dupSub++;
                        logger.LogDebug("Duplicate submitted transaction entry found. Existing {0} New {1} ", sellerSubmitted[submitted.tid], submitted);
                    }
                }
                foreach (var finished in res.Item2)
                {
                    if (!sellerFinished.TryAdd(finished.tid, finished))
                    {
                        dupFin++;
                        logger.LogDebug("Duplicate finished transaction entry found. Existing {0} New {1} ", sellerFinished[finished.tid], finished);
                    }
                }
            }

            foreach (var entry in entries)
            {
                var size = entry.Values[0].Value.ToString().IndexOf("},");
                var str = entry.Values[0].Value.ToString().Substring(8, size - 7);
                var mark = JsonConvert.DeserializeObject<TransactionMark>(str);
                // ok to pay deserialization overhead on metric collection step...
                if (mark.type == TransactionType.CUSTOMER_SESSION) continue;
                if (!sellerFinished.TryAdd(mark.tid, new TransactionOutput(mark.tid, entry.timestamp)))
                {
                    dupFin++;
                    logger.LogDebug("Duplicate finished transaction entry found: Id {0} Existing {1} New {2}", entry.Id, sellerFinished[mark.tid], mark);
                }
            }

            if(dupSub > 0)
                logger.LogWarning("[Seller] Number of duplicated submitted transactions found: {0}", dupSub);
            if (dupFin > 0)
                logger.LogWarning("[Seller] Number of duplicated finished transactions found: {0}", dupFin);

            return BuildLatencyList(sellerSubmitted, sellerFinished, finishTime, "seller");
        }

        private async Task<List<Latency>> CollectFromCustomer(List<Entry> entries, DateTime finishTime)
        {
            int dupSub = 0;
            int dupFin = 0;
            List<Task<List<TransactionIdentifier>>> taskList = new();
            foreach (var customer in customers)
            {
                var customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                taskList.Add(customerWorker.Collect());
            }
            await Task.WhenAll(taskList);

            Dictionary<long, TransactionIdentifier> customerSubmitted = new();
            Dictionary<long, TransactionOutput> customerFinished = new();

            foreach (var task in taskList)
            {
                var res = task.Result;
                foreach (var submitted in res)
                {
                    if(!customerSubmitted.TryAdd(submitted.tid, submitted))
                    {
                        dupSub++;
                        logger.LogDebug("Duplicate submitted transaction entry found. Existing {0} New {1} ", customerSubmitted[submitted.tid], submitted);
                    }
                }
            }

            foreach (var entry in entries)
            {
                var size = entry.Values[0].Value.ToString().IndexOf("},");
                var str = entry.Values[0].Value.ToString().Substring(8, size - 7);
                var mark = JsonConvert.DeserializeObject<TransactionMark>(str);
                if (mark.type != TransactionType.CUSTOMER_SESSION) continue;
                if (!customerFinished.TryAdd(mark.tid, new TransactionOutput(mark.tid, entry.timestamp)))
                {
                    dupFin++;
                    logger.LogDebug("Duplicate finished transaction entry found: Id {0} Mark {1}", entry.Id, mark);
                }
            }

            if (dupSub > 0)
                logger.LogWarning("[Customer] Number of duplicated submitted transactions found: {0}", dupSub);
            if (dupFin > 0)
                logger.LogWarning("[Customer] Number of duplicated finished transactions found: {0}", dupFin);

            return BuildLatencyList(customerSubmitted, customerFinished, finishTime, "customer");
        }

        private async Task<List<Latency>> CollectFromDelivery(DateTime finishTime)
        {
            int dupSub = 0;
            int dupFin = 0;
            var worker = this.orleansClient.GetGrain<IDeliveryProxy>(0);
            var res = await worker.Collect();
            Dictionary<long, TransactionIdentifier> deliverySubmitted = new();
            Dictionary<long, TransactionOutput> deliveryFinished = new();
            foreach (var submitted in res.Item1)
            {
                if(!deliverySubmitted.TryAdd(submitted.tid, submitted))
                {
                    dupSub++;
                }
            }
            foreach (var finished in res.Item2)
            {
                if(!deliveryFinished.TryAdd(finished.tid, finished))
                {
                    dupFin++;
                }
            }

            if (dupSub > 0)
                logger.LogWarning("[Delivery] Number of duplicated submitted transactions found: {0}", dupSub);
            if (dupFin > 0)
                logger.LogWarning("[Delivery] Number of duplicated finished transactions found: {0}", dupFin);

            return BuildLatencyList(deliverySubmitted, deliveryFinished, finishTime, "delivery");
        }

        public async Task Collect(DateTime startTime, DateTime finishTime, int epochPeriod = 0, string runName = null)
        {

            logger.LogInformation("[MetricGather] Starting collecting metrics for run between {0} and {1}", startTime, finishTime);

            StreamWriter sw;
            if (runName is not null)
                sw = new StreamWriter(string.Format("results_{0}.txt", runName));
            else
            {
                string unixTimeMilliSeconds = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
                sw = new StreamWriter(string.Format("results_{0}.txt", unixTimeMilliSeconds));
            }
            sw.WriteLine("Run from {0} to {1}", startTime, finishTime);
            sw.WriteLine("===========================================");

            List<Entry> entries = new();
            while (Shared.FinishedTransactions.Reader.TryRead(out var entry))
            {
                entries.Add(entry);
            }

            // seller workers
            var sellerLatencyList = await CollectFromSeller(entries, finishTime);
            // customer workers
            var customerLatencyList = await CollectFromCustomer(entries, finishTime);
            // delivery worker
            var deliveryLatencyList = await CollectFromDelivery(finishTime);

            var latencyGatherResults = new List<List<Latency>>
            {
                sellerLatencyList,
                customerLatencyList,
                deliveryLatencyList
            };

            Dictionary<TransactionType, List<double>> latencyCollPerTxType = new();

            var txTypeValues = Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>().ToList();
            foreach (var txType in txTypeValues)
            {
                latencyCollPerTxType.Add(txType, new List<double>());
            }

            int countTid = 0;
            foreach (var list in latencyGatherResults)
            {
                foreach (var entry in list)
                {
                    latencyCollPerTxType[entry.type].Add(entry.totalMilliseconds);
                }
                countTid += list.Count;
            }

            foreach (var entry in latencyCollPerTxType)
            {
                double avg = 0;
                if (entry.Value.Count > 0)
                {
                    avg = entry.Value.Average();
                }
                logger.LogInformation("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
                sw.WriteLine("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
            }

            // transactions per second
            TimeSpan executionTime = finishTime - startTime;
            double txPerSecond = countTid / executionTime.TotalSeconds;

            logger.LogInformation("Number of seconds: {0}", executionTime.TotalSeconds);
            sw.WriteLine("Number of seconds: {0}", executionTime.TotalSeconds);
            logger.LogInformation("Number of completed transactions: {0}", countTid);
            sw.WriteLine("Number of completed transactions: {0}", countTid);
            logger.LogInformation("Transactions per second: {0}", txPerSecond);
            sw.WriteLine("Transactions per second: {0}", txPerSecond);
            sw.WriteLine("===========================================");

            // TODO calculate percentiles
            if (epochPeriod > 0)
            {

                // break down latencies by end timestamp
                int blocks = (int)executionTime.TotalMilliseconds / epochPeriod;
                logger.LogInformation("{0} blocks for epoch {1}", blocks, epochPeriod);
                sw.WriteLine("{0} blocks for epoch {1}", blocks, epochPeriod);

                List<Dictionary<TransactionType, List<double>>> breakdown = new(blocks);
                for (int i = 0; i < blocks; i++)
                {
                    breakdown.Add(new());
                    foreach (var txType in txTypeValues)
                    {
                        breakdown[i].Add(txType, new List<double>());
                    }
                }
                // iterate over all results and 
                foreach (var list in latencyGatherResults)
                {
                    foreach (var entry in list)
                    {
                        // find the block the entry belongs to
                        var span = entry.endTimestamp.Subtract(startTime);
                        int idx = (int)(span.TotalMilliseconds / epochPeriod);
                        if (idx < 0 || idx >= breakdown.Count)
                        {
                            logger.LogWarning("Entry outside breakdown boundary. Finish time is {0} and the entry is {1}", finishTime, entry);
                            continue;
                        }
                        breakdown[idx][entry.type].Add(entry.totalMilliseconds);
                    }
                }

                int blockIdx = 1;
                foreach (var block in breakdown)
                {

                    logger.LogInformation("Block {0} results:", blockIdx);
                    sw.WriteLine("Block {0} results:", blockIdx);

                    // iterating over each transaction type in block
                    int blockCountTid = 0;
                    foreach (var entry in block)
                    {
                        double avg = 0;
                        if (entry.Value.Count > 0)
                        {
                            avg = entry.Value.Average();
                        }
                        blockCountTid += entry.Value.Count;
                        logger.LogInformation("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
                        sw.WriteLine("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
                    }

                    double blockTxPerSecond = blockCountTid / (epochPeriod / 1000);

                    logger.LogInformation("Number of completed transactions: {0}", blockCountTid);
                    sw.WriteLine("Number of completed transactions: {0}", blockCountTid);
                    logger.LogInformation("Transactions per second: {0}", blockTxPerSecond);
                    sw.WriteLine("Transactions per second: {0}", blockTxPerSecond);

                    blockIdx++;
                    sw.WriteLine("===========================================");

                }

            }

            // prometheus:
            if (collectionConfig is null)
            {
                goto end_of_file;
            }

            // check whether prometheus is online
            string urlMetric = collectionConfig.baseUrl + "/" + collectionConfig.ready;
            logger.LogInformation("Contacting metric collection API healthcheck on {0}", urlMetric);
            HttpRequestMessage healthCheckMsg = new HttpRequestMessage(HttpMethod.Get, urlMetric);

            try
            {
                HttpResponseMessage resp = HttpUtils.client.Send(healthCheckMsg);

                // if so, get number of messages
                if (!resp.IsSuccessStatusCode)
                {
                    logger.LogError("It was not possible to contact metric collection API healthcheck on {0}", urlMetric);
                    goto end_of_file;
                }

                // https://briancaos.wordpress.com/2022/02/24/c-datetime-to-unix-timestamps/
                DateTimeOffset dto = new DateTimeOffset(finishTime);
                string unixTimeMilliSeconds = dto.ToUnixTimeMilliseconds().ToString();
                var colls = await GetFromPrometheus(collectionConfig, unixTimeMilliSeconds);

                sw.WriteLine("Ingress metrics:");
                foreach (var entry in colls.ingressCountPerMs)
                {
                    sw.WriteLine("App: {0} Count: {1}", entry.Key, entry.Value);
                }
                sw.WriteLine("===========================================");
                sw.WriteLine("Egress metrics:");
                foreach (var entry in colls.egressCountPerMs)
                {
                    sw.WriteLine("App: {0} Count: {1}", entry.Key, entry.Value);
                }

            }
            catch (Exception e)
            {
                logger.LogError("It was not possible to contact metric collection API healthcheck on {0}. Error: {1}", urlMetric, e.Message);
            }

        end_of_file:

            sw.WriteLine("=================    THE END   ================");
            sw.Flush();
            sw.Close();

            logger.LogInformation("[MetricGather] Finished collecting metrics.");

        }

        private static long GetCount(string respStr)
        {
            dynamic respObj = JsonConvert.DeserializeObject<ExpandoObject>(respStr);

            if (respObj.data.result.Count == 0) return 0;

            ExpandoObject data = respObj.data.result[0];
            string count = (string)((List<object>)data.ElementAt(1).Value)[1];
            return Int64.Parse(count);
        }

        /**
         * Made it like this to allow for quick tests in main()
         */
        public static async Task<(Dictionary<string, long> ingressCountPerMs, Dictionary<string, long> egressCountPerMs)>
            GetFromPrometheus(CollectionConfig collectionConfig, string time)
        {
            // collect data from prometheus. some examples below:
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_ingress_count
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count&name=ReserveInventory
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart"}&time=1688136844
            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id="cart",topic="ReserveStock"}&time=1688136844

            var ingressCountPerMs = new Dictionary<string, long>();
            HttpRequestMessage message;
            HttpResponseMessage resp;
            string respStr;
            string query;
            long count;
            foreach (var entry in collectionConfig.ingress_topics)
            {
                // query = string.Format("{app_id=\"{0}\",topic=\"{1}\"}&time={2}", entry.app_id, entry.topic, unixTimeMilliSeconds);
                query = new StringBuilder("{app_id='").Append(entry.app_id).Append("',topic='").Append(entry.topic).Append("'}").Append("&time=").Append
                    (time).ToString();
                message = new HttpRequestMessage(HttpMethod.Get, collectionConfig.baseUrl + "/" + collectionConfig.ingress_count + query);
                resp = HttpUtils.client.Send(message);
                respStr = await resp.Content.ReadAsStringAsync();
                count = GetCount(respStr);

                if (ingressCountPerMs.ContainsKey(entry.app_id))
                {
                    var curr = ingressCountPerMs[entry.app_id];
                    ingressCountPerMs[entry.app_id] = curr + count;
                }
                else
                {
                    ingressCountPerMs.Add(entry.app_id, count);
                }
            }

            var egressCountPerMs = new Dictionary<string, long>();
            foreach (var entry in collectionConfig.egress_topics)
            {
                // query = string.Format("{app_id=\"{0}\",topic=\"{1}\"}&time={2}", entry.app_id, entry.topic, unixTimeMilliSeconds);
                query = new StringBuilder("{app_id='").Append(entry.app_id).Append("',topic='").Append(entry.topic).Append("'}").Append("&time=").Append
                       (time).ToString();
                message = new HttpRequestMessage(HttpMethod.Get, collectionConfig.baseUrl + "/" + collectionConfig.egress_count + query);
                resp = HttpUtils.client.Send(message);
                respStr = await resp.Content.ReadAsStringAsync();
                count = GetCount(respStr);

                if (egressCountPerMs.ContainsKey(entry.app_id))
                {
                    var curr = egressCountPerMs[entry.app_id];
                    egressCountPerMs[entry.app_id] = curr + count;
                }
                else
                {
                    egressCountPerMs.Add(entry.app_id, count);
                }
            }

            return (ingressCountPerMs, egressCountPerMs);

        }

    }
}

