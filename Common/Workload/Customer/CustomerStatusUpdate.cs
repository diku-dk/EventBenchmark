using System;
using Common.Workload.Customer;

namespace Common.Streaming
{
	public record CustomerStatusUpdate
	(long customerId,  CustomerWorkerStatus status);
}

