namespace Common.Workload.Metrics
{
    /**
	 * Returned by worker grains
	 */
    public record TransactionOutput
	(
		string tid,
		DateTime timestamp
	);
}