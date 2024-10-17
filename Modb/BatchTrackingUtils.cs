using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Threading;
using Common.Workload;

namespace Modb;

public sealed class BatchTrackingUtils
{
	// relative to this specific experiment
    // do not necessarily match MODB batch ID
    static long batchId;

    public static IDictionary<long, DateTime> batchToFinishedTsMap;
	public static IDictionary<string, (int workerId, TransactionType transactionType, long batchId)> tidToBatchMap;

	static BatchTrackingUtils()
	{
		batchId = 1;
        batchToFinishedTsMap = new Dictionary<long, DateTime>();
		tidToBatchMap = new ConcurrentDictionary<string, (int,TransactionType,long)>();
	}

	public static void Reset()
	{
		batchId = 1;
		batchToFinishedTsMap.Clear();
		tidToBatchMap.Clear();
	}

	public static void UpdateBatchId(DateTime receivedTs)
	{
		var currBatchId = Interlocked.Read(ref batchId);
        batchToFinishedTsMap.Add(currBatchId, receivedTs);
        currBatchId++;
        Interlocked.Exchange(ref batchId, currBatchId);
	}

	public static void MapTidToBatch(string tid, int workerId, TransactionType transactionType)
	{
		tidToBatchMap.Add(tid, (workerId, transactionType, Interlocked.Read(ref batchId)));
	}

}

