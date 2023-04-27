using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Entity;
using Marketplace.Infra;
using Orleans;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;
using System.Linq;

namespace Marketplace.Actor
{

    public class CustomerActor : Grain, ICustomerActor
	{

        private readonly Dictionary<long, Customer> customers;
        private long customerActorId;
        private int nOrderPartitions;
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
            var mgmt = GrainFactory.GetGrain<IMetadataGrain>(0);
            var dict = await mgmt.GetActorSettings(new List<string>() { "OrderActor" });
            this.nOrderPartitions = dict["OrderActor"];
        }

        public Task AddCustomer(Customer customer)
        {
            this._logger.LogWarning("Attempt to add customer {0} in customer actor {1}", customer.id, this.customerActorId);
            return Task.FromResult(this.customers.TryAdd(customer.id, customer));
        }

        public Task<Customer> GetCustomer(long customerId)
        {
            this._logger.LogWarning("Attempt to retrieve customer id {0} in customer actor {1}", customerId, this.customerActorId);
            return Task.FromResult(this.customers[customerId]);
        }

        public async Task<List<Order>> GetOrders(long customerId, Predicate<Order> predicate = null)
        {
            List<Order> orders = new();
            List<Task<List<Order>>> tasks = new(this.nOrderPartitions);

            for (int i = 0; i < nOrderPartitions; i++)
            {
                tasks.Add(GrainFactory.GetGrain<IOrderActor>(i).GetOrders(customerId, predicate));
            }

            await Task.WhenAll(tasks);

            for (int i = 0; i < nOrderPartitions; i++)
            {
                orders.AddRange(tasks[i].Result);
            }

            return orders;
        }     

        public Task NotifyDelivery(long customerId)
        {
            this.customers[customerId].delivery_count++;
            this.customers[customerId].pending_deliveries_count--;
            return Task.CompletedTask;
        }

        public Task NotifyFailedPayment(long customerId, Order order)
        {
            this.customers[customerId].failed_payment_count++;
            return Task.CompletedTask;
        }

        public Task NotifyPayment(long customerId, Order order)
        {
            this.customers[customerId].success_payment_count++;
            this.customers[customerId].total_spent_items += order.total_items;
            this.customers[customerId].total_spent_freights += order.total_freight;
            return Task.CompletedTask;
        }

        public Task NotifyShipment(long customerId, int numDeliveries)
        {
            this.customers[customerId].pending_deliveries_count += numDeliveries;
            return Task.CompletedTask;
        }
    }
}

