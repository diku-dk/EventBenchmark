using System;
using Common.Entities;
using Common.Http;
using Common.Workload.Metrics;
using GrainInterfaces.Workers;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Orleans;
using Newtonsoft.Json;
using System.Dynamic;
using System.Linq;
using Common.Infra;
using Common.Workload;
using System.IO;
using System.Text;

namespace Client.Collection
{
	public class MetricGather
	{
        private readonly IClusterClient orleansClient;
        private readonly List<Customer> customers;
        private readonly long numSellers;
        private readonly CollectionConfig collectionConfig;
        private readonly bool endToEndLatencyCollection;
        private readonly ILogger logger;

        public MetricGather(IClusterClient orleansClient, List<Customer> customers, long numSellers,
            CollectionConfig collectionConfig, bool endToEndLatencyCollection)
		{
            this.orleansClient = orleansClient;
            this.customers = customers;
            this.numSellers = numSellers;
            this.collectionConfig = collectionConfig;
            this.endToEndLatencyCollection = endToEndLatencyCollection;
            this.logger = LoggerProxy.GetInstance("MetricGather");
        }

		public async Task Collect(DateTime startTime, DateTime finishTime)
		{

            StreamWriter sw = new StreamWriter(string.Format("results_{0}_{1}.txt", startTime.Millisecond, finishTime.Millisecond));

            sw.WriteLine("Run from {0} to {1}", startTime, finishTime);
            sw.WriteLine("===========================================");

            // this is the end to end latency
            if (this.endToEndLatencyCollection)
            {
                // collect() stops subscription to redis streams in every worker

                var latencyGatherTasks = new List<Task<List<Latency>>>();

                foreach (var customer in customers)
                {
                    var customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(customer.id);
                    latencyGatherTasks.Add(customerWorker.Collect(startTime));
                }

                for (int i = 1; i <= numSellers; i++)
                {
                    var sellerWorker = this.orleansClient.GetGrain<ISellerWorker>(i);
                    latencyGatherTasks.Add(sellerWorker.Collect(startTime));
                }

                latencyGatherTasks.Add(this.orleansClient.GetGrain<ISellerWorker>(0).Collect(startTime));

                await Task.WhenAll(latencyGatherTasks);

                Dictionary<TransactionType, List<int>> latencyCollPerTxType = new Dictionary<TransactionType, List<int>>();

                var txTypeValues = Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>().ToList();
                foreach(var txType in txTypeValues)
                {
                    latencyCollPerTxType.Add(txType, new List<int>());
                }

                int maxTid = 0;
                foreach (var list in latencyGatherTasks)
                {
                    foreach(var entry in list.Result)
                    {
                        latencyCollPerTxType[entry.type].Add(entry.period);
                        if (entry.tid > maxTid) maxTid = entry.tid;
                    }
                }

                // throughput
                // getting the Shared.Workload.Take().tid - 1 does not mean the system has finished processing it
                // therefore, we need to get the last (i.e., maximum) tid processed from the grains

                // transactions per second
                TimeSpan timeSpan = finishTime - startTime;
                int txPerSecond = timeSpan.Seconds / maxTid;

                sw.WriteLine("Number of seconds: {0}", timeSpan.Seconds);
                sw.WriteLine("Number of completed transactions: {0}", maxTid);
                sw.WriteLine("Transactions per second: {0}", txPerSecond);
                sw.WriteLine("===========================================");
            }

            // check whether prometheus is online
            string urlMetric = collectionConfig.baseUrl + "/" + collectionConfig.ready;
            logger.LogInformation("Contacting metric collection API healthcheck on {1}", urlMetric);
            HttpRequestMessage healthCheckMsg = new HttpRequestMessage(HttpMethod.Get, urlMetric);
            HttpResponseMessage resp = HttpUtils.client.Send(healthCheckMsg);

            // if so, get number of messages
            if (resp.IsSuccessStatusCode)
            {
                // https://briancaos.wordpress.com/2022/02/24/c-datetime-to-unix-timestamps/
                DateTimeOffset dto = new DateTimeOffset(finishTime);
                string unixTimeMilliSeconds = dto.ToUnixTimeMilliseconds().ToString();
                var colls = await GetFromPrometheus(collectionConfig, unixTimeMilliSeconds); 

                sw.WriteLine("Ingress metrics:");
                foreach(var entry in colls.ingressCountPerMs)
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
            else
            {
                logger.LogError("It was not possible to contact {0} metric collection API healthcheck on {1}", urlMetric);
            }

            sw.WriteLine("===========    THE END   ==============");
            sw.Close();

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

