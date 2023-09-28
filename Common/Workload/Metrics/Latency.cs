namespace Common.Workload.Metrics
{
	public record Latency
	(
        string tid,
		TransactionType type,
		double totalMilliseconds,
		DateTime endTimestamp
	);
}

