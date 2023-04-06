using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Marketplace.Entity;
using Marketplace.Infra;
using Orleans;

namespace Marketplace.Actor
{
    public interface ICustomerActor : IGrainWithIntegerKey, SnapperActor
    {
		public Task<Customer> GetCustomer(long customerId);
        public Task IncrementSuccessfulPayments(long customerId);
        public Task IncrementFailedPayments(long customerId);

        public Task IncrementDeliveryCount(long customerId);

        // API
        public Task AddCustomer(Customer customer);
    }

    public class CustomerActor : Grain, ICustomerActor
	{

        private Dictionary<long, Customer> customers;

        public CustomerActor()
		{
            this.customers = new();
        }

        public Task AddCustomer(Customer customer)
        {
            return Task.FromResult(this.customers.TryAdd(customer.customer_id, customer));
        }

        public Task<Customer> GetCustomer(long customerId)
        {
            return Task.FromResult(this.customers[customerId]);
        }

        public Task IncrementDeliveryCount(long customerId)
        {
            this.customers[customerId].delivery_count++;
            return Task.CompletedTask;
        }

        public Task IncrementFailedPayments(long customerId)
        {
            this.customers[customerId].failed_payment_count++;
            return Task.CompletedTask;
        }

        public Task IncrementSuccessfulPayments(long customerId)
        {
            this.customers[customerId].failed_payment_count++;
            return Task.CompletedTask;
        }
    }
}

