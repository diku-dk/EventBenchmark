namespace Common.Workload.Metrics
{
	/**
	 * Tracked by emitter.
	 * Later matched with transaction output
	 */
	public record TransactionIdentifier
	(
		string tid,
		TransactionType type,
		DateTime timestamp
	);
}