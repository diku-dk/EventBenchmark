using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Common.Entity;
using Marketplace.Infra;
using Orleans;
using Marketplace.Interfaces;

namespace Marketplace.Actor
{

    public class CustomerActor : Grain, ICustomerActor
	{

        private Dictionary<long, Customer> customers;
        // private Dictionary<long, string> notifications; // or customer log
        // type, json (differs depending on the type). Types: invoiced?, payment, shipment, delivery
        // for package delivery: shipment_id, package_id, 

        public CustomerActor()
		{
            this.customers = new();
        }

        public Task AddCustomer(Customer customer)
        {
            return Task.FromResult(this.customers.TryAdd(customer.id, customer));
        }

        public Task<Customer> GetCustomer(long customerId)
        {
            return Task.FromResult(this.customers[customerId]);
        }

        public Task NotifyDelivery(long customerId)
        {
            this.customers[customerId].delivery_count++;
            return Task.CompletedTask;
        }

        public Task NotifyPayment(long customerId, Order order = null, bool success = true)
        {
            if (success) { 
                this.customers[customerId].success_payment_count++;
                this.customers[customerId].total_spent_items += order.total_items;
                this.customers[customerId].total_spent_freights += order.total_freight;
            }
            else { 
                this.customers[customerId].failed_payment_count++;
            }


            return Task.CompletedTask;
        }
    }
}

