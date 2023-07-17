using System.Collections.Concurrent;
using System.Threading.Channels;
using Common.Workload.Seller;

namespace Client.Workload
{
	public sealed class Shared
	{
        public static readonly BlockingCollection<object> WaitHandle = new BlockingCollection<object>();
        public static readonly BlockingCollection<TransactionInput> Workload = new BlockingCollection<TransactionInput>();

        public static readonly Channel<int> ResultQueue = Channel.CreateUnbounded<int>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    }
}