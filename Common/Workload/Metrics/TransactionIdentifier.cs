namespace Common.Workload.Metrics
{
	/**
	 * Tracked by emitter.
	 * Later matched with transaction output
	 */
	public record TransactionIdentifier
	(
		int tid,
		TransactionType type,
		DateTime timestamp
	);
}