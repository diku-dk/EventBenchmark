using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Common.Ingestion.Config;
using Common.Http;
using Common.Infra;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Common.Ingestion
{

	public class IngestionOrchestrator
	{

		private readonly IngestionConfig config;
        private static readonly ILogger logger = LoggerProxy.GetInstance(nameof(IngestionOrchestrator));

        public IngestionOrchestrator(IngestionConfig config)
		{
			this.config = config;
            if(this.config.concurrencyLevel == 0)
            {
                this.config.concurrencyLevel = Environment.ProcessorCount;
                logger.LogInformation("Set concurrency level of ingestion to processor count: {0}", this.config.concurrencyLevel);
            }
        }

		public async Task Run(DuckDBConnection connection)
		{
            var startTime = DateTime.UtcNow;
            logger.LogInformation("Ingestion process starting at {0} with strategy {1}", startTime, config.strategy.ToString());

            var command = connection.CreateCommand();

            List<Task> tasksToWait = new();

            foreach (var table in config.mapTableToUrl)
            {
                logger.LogInformation("Ingesting table {0} at {1}", table, DateTime.UtcNow);

                command.CommandText = "select * from "+table.Key+";";
                var queryResult = command.ExecuteReader();

                BlockingCollection<JObject> tuples = new BlockingCollection<JObject>();

                Task t1 = Task.Run(() => Produce(tuples, queryResult));

                long rowCount = GetRowCount(queryResult);

                if(rowCount == 0)
                {
                    logger.LogWarning("Table {0} is empty!", table);
                    continue;
                }

                if (config.strategy == IngestionStrategy.TABLE_PER_WORKER)
                {
                    TaskCompletionSource tcs = new TaskCompletionSource();
                    Task t = Task.Run(() => Consume(tuples, table.Value, rowCount, tcs));
                    tasksToWait.Add(tcs.Task);
                }
                else if (config.strategy == IngestionStrategy.WORKER_PER_CPU)
                {

                    for (int i = 0; i < config.concurrencyLevel; i++) {
                        TaskCompletionSource tcs = new TaskCompletionSource();
                        Task t = Task.Run(() => ConsumeShared(tuples, table.Value, rowCount, tcs));
                        tasksToWait.Add(tcs.Task);
                    }
                    await Task.WhenAll(tasksToWait);
                    totalCount = 0;
                    tasksToWait.Clear();
                    logger.LogInformation("Finished loading table {0} at {1}", table, DateTime.UtcNow);
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

            TimeSpan span = DateTime.UtcNow - startTime;
            logger.LogInformation("Ingestion process has terminated in {0} seconds", span.TotalSeconds);
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
            logger.LogDebug("Ingestion worker ID {0} has started", Environment.CurrentManagedThreadId);
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
            logger.LogDebug("Ingestion worker ID {0} has finished", Environment.CurrentManagedThreadId);
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

