using System;

namespace Common.Workload
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