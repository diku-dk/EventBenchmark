using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;
using System.Xml.Linq;
using Common.Http;
using Common.Ingestion.Config;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static DuckDB.NET.NativeMethods;

namespace Client.Ingestion
{
	public class IngestionOrchestrator
	{

		private readonly IngestionConfiguration config;
        private readonly BlockingCollection<string> tuples;

        public IngestionOrchestrator(IngestionConfiguration config)
		{
			this.config = config;
            this.tuples = new BlockingCollection<string>();
        }

		public void Run()
		{
            Console.WriteLine("Ingestion process is about to start.");

            using (var duckDBConnection = new DuckDBConnection(config.connectionString))
            {
                duckDBConnection.Open();
                var command = duckDBConnection.CreateCommand();

                foreach (var table in config.mapTableToUrl)
                {
                    command.CommandText = "select * from "+table.Key+";";
                    var queryResult = command.ExecuteReader();

                    Task t1 = Task.Run(() => Produce(queryResult));

                    long rowCount = GetRowCount(queryResult);

                    Task t2 = Task.Run(() => Consume(table.Value, rowCount));

                    Task.WaitAll(t1, t2);
                }

            }

            Console.WriteLine("Ingestion process has terminated.");
        }

        private void Produce(DuckDBDataReader queryResult)
        {
            JObject obj = new JObject();
            while (queryResult.Read())
            {
               
                for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
                {
                    var column = queryResult.GetName(ordinal);
                    var val = queryResult.GetValue(ordinal);
                    obj[column] = val.ToString();
                }

                string strObj = JsonConvert.SerializeObject(obj);
                // Console.WriteLine(strObj);
                this.tuples.Add(strObj);
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
            string obj = null;
            do
            {

                obj = this.tuples.Take();
               
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url);
                message.Content = HttpUtils.BuildPayload(obj);

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

