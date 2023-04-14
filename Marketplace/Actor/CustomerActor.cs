using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Scenario.Entity;
using Common.Entity;
using Marketplace.Infra;
using Orleans;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Marketplace.Actor
{

    public class CustomerActor : Grain, ICustomerActor
	{

        private Dictionary<long, Customer> customers;
        private long customerActorId;

        // private Dictionary<long, string> notifications; // or customer log
        // type, json (differs depending on the type). Types: invoiced?, payment, shipment, delivery
        // for package delivery: shipment_id, package_id, 
        private readonly ILogger<CustomerActor> _logger;

        public CustomerActor(ILogger<CustomerActor> _logger)
		{
            this.customers = new();
            this._logger = _logger;
        }

        public override async Task OnActivateAsync()
        {
            this.customerActorId = this.GetPrimaryKeyLong();
            await base.OnActivateAsync();
        }

        public Task AddCustomer(Customer customer)
        {
            _logger.LogWarning("Attempt to add customer {0} in customer actor {1}", customer.id, this.customerActorId);
            return Task.FromResult(this.customers.TryAdd(customer.id, customer));
        }

        public Task<Customer> GetCustomer(long customerId)
        {
            _logger.LogWarning("Attempt to retrieve customer id {0} in customer actor {1}", customerId, this.customerActorId);
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

