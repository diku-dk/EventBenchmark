using System;
using Common.Scenario.Entity;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using System.Threading.Tasks;

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
        public Task<Invoice> Checkout_1(Checkout checkout);
        public Task UpdateOrderStatus(long orderId, OrderStatus status);
    }
}

