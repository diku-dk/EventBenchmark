using System;

namespace Common.Workload.Metrics
{
    /**
	 * Returned by worker grains
	 */
    public record TransactionOutput
	(
		int tid,
		DateTime timestamp
	);
}