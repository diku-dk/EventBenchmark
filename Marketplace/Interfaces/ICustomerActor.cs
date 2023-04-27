using System;
using Common.Entity;
using Marketplace.Infra;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Marketplace.Interfaces
{
    public interface ICustomerActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task<Customer> GetCustomer(long customerId);
        public Task NotifyPayment(long customerId, Order order);
        public Task NotifyFailedPayment(long customerId, Order order);

        public Task NotifyShipment(long customerId, int numDeliveries);
        public Task NotifyDelivery(long customerId);

        // API
        public Task AddCustomer(Customer customer);
        public Task<List<Order>> GetOrders(long customerId, Predicate<Order> predicate);
    }
}

