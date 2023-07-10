using System.Collections.Concurrent;
using Common.Workload.Seller;

namespace Client.Workload
{
	public sealed class Shared
	{
        public static readonly BlockingCollection<object> WaitHandle = new BlockingCollection<object>();
        public static readonly BlockingCollection<TransactionInput> Workload = new BlockingCollection<TransactionInput>();

        public static readonly BlockingCollection<object> ResultQueue = new BlockingCollection<object>();
    }
}