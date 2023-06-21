using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Common.Workload;

namespace Client.Workload
{
	public sealed class Shared
	{
        public static readonly AutoResetEvent WaitHandle = new AutoResetEvent(false);
        public static readonly BlockingCollection<TransactionIdentifier> Workload = new BlockingCollection<TransactionIdentifier>();

        public static readonly BlockingCollection<object> ResultQueue = new BlockingCollection<object>();

    }
}

