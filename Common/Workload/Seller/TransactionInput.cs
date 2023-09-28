namespace Common.Workload.Seller
{
	/**
	 * Received by seller worker grains
	 */
	public record TransactionInput
	(
		string tid,
		TransactionType type
	);
}