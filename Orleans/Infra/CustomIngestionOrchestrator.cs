using System.Collections.Concurrent;
using Common.Ingestion.Config;
using Common.Http;
using Common.Infra;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Orleans.Infra;

public sealed class CustomIngestionOrchestrator
{

	public static async Task Run(DuckDBConnection connection, IngestionConfig config)
	{
        var startTime = DateTime.UtcNow;
        var numThreads = config.concurrencyLevel <= 0 ? Environment.ProcessorCount : config.concurrencyLevel;
        Console.WriteLine("Ingestion process starting at {0} with strategy {1} and numWorkers {2}", startTime, config.strategy.ToString(),numThreads);

        var command = connection.CreateCommand();

        List<Task> tasksToWait = new();

        int total =  config.mapTableToUrl.Count;

        BlockingCollection<(JObject,string Url)> tuples = new BlockingCollection<(JObject,string Url)>();

        BlockingCollection<(string,string)> errors = new BlockingCollection<(string,string)>();

        ConsoleUtility.WriteProgressBar(0);

        foreach (var table in config.mapTableToUrl)
        {
            command.CommandText = "select * from "+table.Key+";";
            var queryResult = command.ExecuteReader();
            tasksToWait.Add(Task.Run(() => Produce(tuples, queryResult, table.Value)));
        }

        await Task.WhenAll(tasksToWait);
        ConsoleUtility.WriteProgressBar(50, true);
        tasksToWait.Clear();

        for (int i = 0; i < numThreads; i++) {
             tasksToWait.Add( Task.Run(() => ConsumeShared(tuples, errors)) );
        }
        
        await Task.WhenAll(tasksToWait);

        ConsoleUtility.WriteProgressBar(100, true);
        Console.WriteLine();
        Console.WriteLine("Finished loading all tables.");

        if(errors.Count > 0)
        {
            Console.WriteLine("Errors found while ingesting. An example:");
            var errorEntry = errors.Take();
            Console.WriteLine(errorEntry.Item1+" "+errorEntry.Item2);
        }

        TimeSpan span = DateTime.UtcNow - startTime;
        Console.WriteLine("Ingestion process has terminated in {0} seconds", span.TotalSeconds);
    }

    private static void Produce(BlockingCollection<(JObject,string Url)> tuples, DuckDBDataReader queryResult, string Url)
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
            tuples.Add((obj,Url));
        }
    }

    private static void ConsumeShared(BlockingCollection<(JObject Tuple, string Url)> tuples, BlockingCollection<(string,string)> errors)
    {
        while (tuples.TryTake(out (JObject Tuple, string Url) item))
        {
            try
            {
                ConvertAndSend(item.Tuple, item.Url);
            }
            catch (Exception e)
            {
                errors.Add((item.Url,e.Message));
            }
        }
    }

    private static void ConvertAndSend(JObject obj, string url)
    {
        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = HttpUtils.BuildPayload(JsonConvert.SerializeObject(obj))
        };
        HttpUtils.client.Send(message, HttpCompletionOption.ResponseHeadersRead);
    }

}

