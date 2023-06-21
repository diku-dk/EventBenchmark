using System;
using Common.Workload;

namespace Common.Workload
{
	public record TransactionIdentifier
	(
		int tid,
		TransactionType type

	);
}

