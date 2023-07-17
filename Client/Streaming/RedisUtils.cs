using Common.Infra;
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
    }

}