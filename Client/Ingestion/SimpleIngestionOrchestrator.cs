using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Reflection.Emit;
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
        private readonly BlockingCollection<JObject> tuples;

        public SimpleIngestionOrchestrator(IngestionConfig config)
		{
			this.config = config;
            this.tuples = new BlockingCollection<JObject>();
        }

		public void Run()
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

                    Task t1 = Task.Run(() => Produce(queryResult));

                    long rowCount = GetRowCount(queryResult);
                    Task t2 = Task.Run(() => Consume(table.Value, rowCount));

                    // spawning several threads leads to corrupted state. guess it is related to the shared queue...
                    if (config.distributionStrategy == IngestionDistributionStrategy.SINGLE_WORKER)
                    {
                        Task.WaitAll(t1, t2);
                        Console.WriteLine("Finished loading table {0}", table);
                    }
                    else
                    {
                        tasksToWait.Add(t1);
                        tasksToWait.Add(t2);
                    }
 
                }

                if(tasksToWait.Count > 0)
                {
                    Task.WhenAll(tasksToWait);
                    Console.WriteLine("Finished loading all tables");
                }

            }

            Console.WriteLine("Ingestion process has terminated.");
        }

        private void Produce(DuckDBDataReader queryResult)
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
                this.tuples.Add(obj);
            }
        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

        private long GetRowCount(DuckDBDataReader queryResult)
        {
            var field = queryResult.GetType().GetField("rowCount", bindingFlags);
            return (long)field?.GetValue(queryResult);
        }

        private void Consume(string url, long rowCount) {
            int currRow = 1;
            string strObj = null;
            do
            {
                var obj = this.tuples.Take();
                strObj = JsonConvert.SerializeObject(obj); 
           
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

        }

	}
}

