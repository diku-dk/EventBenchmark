using System;
using Common.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using System.Threading.Tasks;
using Orleans.Concurrency;
using System.Collections.Generic;

namespace Marketplace.Interfaces
{
    /**
     * Order actor does not coordinate with product actors.
     * Order only coordinate with stock actors.
     * This design favors higher useful work per time unit.
     * Since product is a user-facing microservice, most
     * customer requests target the product microservice.
     */
    
    public interface IOrderActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task Checkout(Checkout checkout);
        public Task UpdateOrderStatus(long orderId, OrderStatus status);

        public Task<List<Order>> GetOrders(long customerId, Predicate<Order> predicate);
    }
}

