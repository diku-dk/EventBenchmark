using System;
using Common.Scenario.Customer;

namespace Common.Streaming
{
	public class CustomerStatusUpdate
	{
        public readonly long customerId;

        public readonly CustomerWorkerStatus status;

        public CustomerStatusUpdate(long customerId, CustomerWorkerStatus status) {
            this.customerId = customerId;
            this.status = status;
        }
    }
}

