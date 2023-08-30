using Common.Infra;
using StackExchange.Redis;

namespace Daprr.Streaming.Redis
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
    public sealed class RedisUtils
    {
        public static readonly ILogger logger = LoggerProxy.GetInstance("RedisUtils");

        public static bool TestRedisConnection(string connection)
        {
            using (var db = ConnectionMultiplexer.Connect(connection))
            {
                return db.IsConnected;
            }
        }

        public static async Task TrimStreams(string connection, List<string> streams)
        {
            using (var conn = ConnectionMultiplexer.Connect(connection))
            {
                List<Task> tasks = new(streams.Count);
                var db = conn.GetDatabase();
                foreach (var streamName in streams)
                {
                    var arguments = new List<object>
                    {
                        streamName,
                        "MAXLEN",
                        0
                    };
                    tasks.Add( db.ExecuteAsync("XTRIM", arguments) );
                }
                await Task.WhenAll(tasks);
            }
        }

        public static ConnectionMultiplexer GetConnection(string redisConnection)
        {
            return ConnectionMultiplexer.Connect(redisConnection);
        }
    
        public static async Task SubscribeStream(
                string connection,
                string streamName,
                CancellationToken cancellation,
                Action<Entry> handler)
        {

            // The blocking reader's connection should not be shared with any other operation.
            var redis = ConnectionMultiplexer.Connect(connection);
            if (redis is null)
            {
                logger.LogInformation($"Connection to {connection} failed");
                return;
            }
            logger.LogInformation($"Started consuming from stream {streamName}");

            try
            {
                var db = redis.GetDatabase();

                var currentId = "0-0"; // "0-0" listen from start
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
                    // before responding to a cancellation request
                    var result = await db.ExecuteAsync("XREAD", arguments).ConfigureAwait(false);

                    if (!result.IsNull)
                    {
                        var now = DateTime.UtcNow;
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

                                var entry = new Entry(name, id, pairs, now);
                                handler(entry);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                logger.LogInformation(ex.ToString());
            }
            finally
            {
                logger.LogInformation($"Stopped consuming from stream {streamName}");
            }
        }

        public record Entry(RedisValue StreamName, RedisValue Id, Pair[] Values, DateTime timestamp);

        public record Pair(RedisValue Name, RedisValue Value);
    }
}