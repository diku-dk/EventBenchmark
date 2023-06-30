using System;
using Orleans.TestingHost;
using Marketplace.Test;
using Client.Streaming.Redis;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using Common.Streaming;
using Common.Workload;
using System.Text;
using Tests.Events;

/*
 * if fails, add orleans.codegenerator.msbuild and core.abstractions
 */
namespace Tests
{
    [Collection(ClusterCollection.Name)]
    public class CustomerWorkerTest
    {
        private readonly TestCluster _cluster;

        private readonly Random random = new Random();



        public CustomerWorkerTest(ClusterFixture fixture)
        {
            this._cluster = fixture.Cluster;
        }

        [Fact]
        public async Task BrowseAndCheckout()
        {
            JsonSerializerSettings sett = new JsonSerializerSettings();
            sett.MissingMemberHandling = MissingMemberHandling.Ignore;
            sett.NullValueHandling = NullValueHandling.Ignore;
            
            var channel = new StringBuilder(TransactionType.CUSTOMER_SESSION.ToString()).Append('_').Append(0).ToString();

            CancellationTokenSource token = new CancellationTokenSource();
            Task externalTask = Task.Run(() => RedisUtils.Subscribe("localhost","ReserveStock", token.Token, entry =>
            {
                // This code runs on the thread pool scheduler, not on Orleans task scheduler
                // parse redis value so we can know which transaction has finished
                // finishedTransactions.Add
                var now = DateTime.Now;

                try
                {
                    JObject? d = JsonConvert.DeserializeObject<JObject>(entry.Values[0].Value.ToString());
                    if (d is not null)
                    {
                        var str = d.SelectToken("['data']").ToString();
                        ReserveStock? evt = JsonConvert.DeserializeObject<ReserveStock>(str, sett);
                        Console.WriteLine(evt.instanceId);
                    }
                }catch(Exception e)
                {
                    Console.WriteLine("Error {0}", e.Message);
                }
            }));

            await externalTask;
            
        }
    }
}

