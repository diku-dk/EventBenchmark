using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using Client.Ingestion.Config;
using Common.Http;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Client.Ingestion
{
	public class SimpleIngestionOrchestrator
	{

		private readonly IngestionConfig config;

        public SimpleIngestionOrchestrator(IngestionConfig config)
		{
			this.config = config;
        }

		public async Task Run()
		{
            Console.WriteLine("Ingestion process is about to start.");

            using (var duckDBConnection = new DuckDBConnection(config.connectionString))
            {
                duckDBConnection.Open();
                var command = duckDBConnection.CreateCommand();

                List<Task> tasksToWait = new();

                foreach (var table in config.mapTableToUrl)
                {
                    Console.WriteLine("Ingesting table {0}", table);

                    command.CommandText = "select * from "+table.Key+";";
                    var queryResult = command.ExecuteReader();

                    BlockingCollection<JObject> tuples = new BlockingCollection<JObject>();

                    Task t1 = Task.Run(() => Produce(tuples, queryResult));

                    long rowCount = GetRowCount(queryResult);

                    TaskCompletionSource tcs = new TaskCompletionSource();

                    Task t2 = Task.Run(() => Consume(tuples, table.Value, rowCount, tcs));

                    // spawning several threads leads to corrupted state. guess it is related to the shared queue...
                    if (config.distributionStrategy == IngestionDistributionStrategy.SINGLE_WORKER)
                    {
                        // await Task.WhenAll(t1, t2);
                        await tcs.Task;
                        Console.WriteLine("Finished loading table {0}", table);
                    }
                    else
                    {
                        // tasksToWait.Add(t1);
                        // tasksToWait.Add(t2);
                        tasksToWait.Add(tcs.Task);
                    }
 
                }

                if(tasksToWait.Count > 0)
                {
                    await Task.WhenAll(tasksToWait);
                    Console.WriteLine("Finished loading all tables");
                }

            }

            Console.WriteLine("Ingestion process has terminated.");
        }

        private void Produce(BlockingCollection<JObject> tuples, DuckDBDataReader queryResult)
        {
            while (queryResult.Read())
            {
                JObject obj = new JObject();
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    var column = queryResult.GetName(ordinal);
                    var val = queryResult.GetValue(ordinal);
                    obj[column] = JToken.FromObject(val);
                }
                tuples.Add(obj);
            }
        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        private long GetRowCount(DuckDBDataReader queryResult)
        {
            var field = queryResult.GetType().GetField("rowCount", bindingFlags);
            return (long)field?.GetValue(queryResult);
        }

        private void Consume(BlockingCollection<JObject> tuples, string url, long rowCount, TaskCompletionSource tcs)
        {
            int currRow = 1;
            do
            {
                var obj = tuples.Take();
                string strObj = JsonConvert.SerializeObject(obj); 
           
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url);
                message.Content = HttpUtils.BuildPayload(strObj);

                try
                {
                    using HttpResponseMessage response = HttpUtils.client.Send(message);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Exception message: {0}", e.Message);
                }

                currRow++;
            } while (currRow <= rowCount);
            tcs.SetResult();
        }

	}
}

