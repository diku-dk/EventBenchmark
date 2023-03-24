using System;
using Common.Scenario.Customer;

namespace Common.Streaming
{
	public class CustomerStatusUpdate
	{
        public readonly long customerId;

        public readonly CustomerStatus status;

        public CustomerStatusUpdate(long customerId, CustomerStatus status) {
            this.customerId = customerId;
            this.status = status;
        }
    }
}

