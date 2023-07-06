namespace Common.Workload.Metrics
{
	public record Latency
	(
        int tid,
		TransactionType type,
		double period
	);
}

