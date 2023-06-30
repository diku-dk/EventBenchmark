namespace Common.Workload.Seller
{
	/**
	 * Received by seller worker grains
	 */
	public record TransactionInput
	(
		int tid,
		TransactionType type
	);
}