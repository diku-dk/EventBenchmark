using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Marketplace.Entity;
using Orleans;

namespace Marketplace.Actor
{
    public interface ICustomerActor : IGrainWithIntegerKey
	{
		public Task<Customer> GetCustomer(long customerId);
        public Task IncrementSuccessfulPayments();
        public Task IncrementFailedPayments();

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
            throw new NotImplementedException();
        }

        public Task<Customer> GetCustomer(long customerId)
        {
            throw new NotImplementedException();
        }

        public Task IncrementFailedPayments()
        {
            throw new NotImplementedException();
        }

        public Task IncrementSuccessfulPayments()
        {
            throw new NotImplementedException();
        }
    }
}

