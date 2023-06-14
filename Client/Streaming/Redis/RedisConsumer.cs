using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Client.Infra;
using Client.Workload;
using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace Client.Streaming.Redis
{
    /**
	 * No async stream consumtpion (aka XREAD command) support in Redis C# client:
	 * https://stackexchange.github.io/StackExchange.Redis/Streams
	 * 
	 * Workaround based on:
	 * https://mikehadlow.com/posts/2022-02-18-xread-from-a-redis-stream-using-stackexchange-redis/
	 * 
	 * Details of XREAD API:
	 * https://redis.io/docs/data-types/streams-tutorial/#listening-for-new-items-with-xread
	 */
    public sealed class RedisConsumer
    {

        private readonly ConnectionMultiplexer redis;
        private static readonly ILogger logger = LoggerProxy.GetInstance("RedisConsumer");

        public static bool TestRedisConnection()
        {
            using (var db = ConnectionMultiplexer.Connect("localhost"))
            {
                return db.IsConnected;
            }
        }

        public static Task Subscribe(string stream, CancellationToken cancellation, Action<Entry> handler, string connection = "localhost")
        {
            return BlockingReader.Listen(connection, stream, cancellation, handler);
        }
        
    }

    public static class BlockingReader
    {
        public static async Task Listen(
            string connection,
            string streamName,
            CancellationToken cancellation,
            Action<Entry> handler)
        {
            // The blocking reader's connection should not be shared with any other operation.
            var redis = ConnectionMultiplexer.Connect(connection);
            if (redis is null)
            {
                Console.WriteLine($"Connection to {connection} failed");
                return;
            }
            Console.WriteLine($"Started consuming from stream {streamName}");

            try
            {
                var db = redis.GetDatabase();

                var currentId = "$"; // listen for new messages
                while (!cancellation.IsCancellationRequested)
                {
                    var arguments = new List<object>
                    {
                        "BLOCK",
                        "500", // timeout. 0 never times out
                        "STREAMS",
                        streamName,
                        currentId
                    };

                    // ExecuteAsync does not take a CancellationToken, so we have to wait the block time
                    // before resonding to a cancellation request.
                    var result = await db.ExecuteAsync("XREAD", arguments).ConfigureAwait(false);

                    if (!result.IsNull)
                    {
                        // should only be a single result if querying a single stream
                        foreach (RedisResult[] subresults in (RedisResult[])result)
                        {
                            var name = (RedisValue)subresults[0];
                            foreach (RedisResult[] messages in (RedisResult[])subresults[1])
                            {
                                var id = (RedisValue)messages[0];
                                currentId = id;

                                var nameValuePairs = (RedisResult[])messages[1];
                                var pairs = new Pair[nameValuePairs.Length / 2];

                                for (var i = 0; i < nameValuePairs.Length; i += 2)
                                {
                                    pairs[i / 2] = new Pair((RedisValue)nameValuePairs[i], (RedisValue)nameValuePairs[i + 1]);
                                }

                                var entry = new Entry(name, id, pairs);
                                handler(entry);
                            }
                        }
                    }
                }
            }
            catch (TaskCanceledException) { }
            catch (Exception ex)
            {
                Console.WriteLine(ex.ToString());
            }
            finally
            {
                Console.WriteLine($"Stopped consuming from stream {streamName}");
            }
        }
    }

    public record Entry(RedisValue StreamName, RedisValue Id, Pair[] Values);

    public record Pair(RedisValue Name, RedisValue Value);


}