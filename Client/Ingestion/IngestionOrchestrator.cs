using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Client.Ingestion.Config;
using Common.Http;
using Common.Infra;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Client.Ingestion
{

	public class IngestionOrchestrator
	{

		private readonly IngestionConfig config;
        private static readonly ILogger logger = LoggerProxy.GetInstance("SimpleIngestionOrchestrator");

        public IngestionOrchestrator(IngestionConfig config)
		{
			this.config = config;
        }

		public async Task Run()
		{
            logger.LogInformation("Ingestion process is about to start.");

            using (var duckDBConnection = new DuckDBConnection(config.connectionString))
            {
                duckDBConnection.Open();
                var command = duckDBConnection.CreateCommand();

                List<Task> tasksToWait = new();

                foreach (var table in config.mapTableToUrl)
                {
                    logger.LogInformation("Ingesting table {0}", table);

                    command.CommandText = "select * from "+table.Key+";";
                    var queryResult = command.ExecuteReader();

                    BlockingCollection<JObject> tuples = new BlockingCollection<JObject>();

                    Task t1 = Task.Run(() => Produce(tuples, queryResult));

                    long rowCount = GetRowCount(queryResult);

                    if (config.strategy == IngestionStrategy.TABLE_PER_WORKER)
                    {
                        TaskCompletionSource tcs = new TaskCompletionSource();
                        Task t = Task.Run(() => Consume(tuples, table.Value, rowCount, tcs));
                        tasksToWait.Add(tcs.Task);
                    }
                    else if (config.strategy == IngestionStrategy.DISTRIBUTE_RECORDS_PER_NUM_CPUS)
                    {
                        for (int i = 0; i < config.concurrencyLevel; i++) {
                            TaskCompletionSource tcs = new TaskCompletionSource();
                            Task t = Task.Run(() => ConsumeShared(tuples, table.Value, rowCount, tcs));
                            tasksToWait.Add(tcs.Task);
                        }
                        await Task.WhenAll(tasksToWait);
                        totalCount = 0;
                        tasksToWait.Clear();
                        logger.LogInformation("Finished loading table {0}", table);
                    }
                    else // default to single worker
                    {
                        TaskCompletionSource tcs = new TaskCompletionSource();
                        Task t = Task.Run(() => Consume(tuples, table.Value, rowCount, tcs));
                        await tcs.Task;
                        logger.LogInformation("Finished loading table {0}", table);
                    }
 
                }

                if(tasksToWait.Count > 0)
                {
                    await Task.WhenAll(tasksToWait);
                    logger.LogInformation("Finished loading all tables");
                }

            }

            logger.LogInformation("Ingestion process has terminated.");
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

        int totalCount = 0;

        private void ConsumeShared(BlockingCollection<JObject> tuples, string url, long rowCount, TaskCompletionSource tcs)
        {
            JObject jobject;
            do
            {
                bool taken = tuples.TryTake(out jobject);
                if (taken)
                {
                    Interlocked.Increment(ref totalCount);
                    ConvertAndSend(jobject, url);
                }
            } while (Volatile.Read(ref totalCount) < rowCount);
            tcs.SetResult();
        }

        private void Consume(BlockingCollection<JObject> tuples, string url, long rowCount, TaskCompletionSource tcs)
        {
            int currRow = 1;
            do
            {
                JObject obj = tuples.Take();
                ConvertAndSend(obj, url);
                currRow++;
            } while (currRow <= rowCount);
            tcs.SetResult();
        }

        private void ConvertAndSend(JObject obj, string url)
        {
            string strObj = JsonConvert.SerializeObject(obj);

            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url);
            message.Content = HttpUtils.BuildPayload(strObj);

            try
            {
                using HttpResponseMessage response = HttpUtils.client.Send(message);
            }
            catch (Exception e)
            {
                logger.LogInformation("Exception message: {0}", e.Message);
            }
        }

	}
}

