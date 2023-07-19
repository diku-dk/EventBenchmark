using System.Threading.Channels;
using Client.Streaming.Redis;

namespace Client.Workload
{
	public sealed class Shared
	{

        public static readonly Channel<RedisUtils.Entry> FinishedTransactions = Channel.CreateUnbounded<RedisUtils.Entry>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<byte> ResultQueue = Channel.CreateUnbounded<byte>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    }
}