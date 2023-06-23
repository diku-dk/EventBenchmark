using System.Collections.Concurrent;
using System.Threading;
using Common.Workload;

namespace Client.Workload
{
	public sealed class Shared
	{
        public static readonly AutoResetEvent WaitHandle = new AutoResetEvent(false);
        public static readonly BlockingCollection<TransactionInput> Workload = new BlockingCollection<TransactionInput>();

        public static readonly BlockingCollection<object> ResultQueue = new BlockingCollection<object>();
    }
}