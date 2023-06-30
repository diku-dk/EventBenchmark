using System;
using Common.Entities;
using Common.Requests;

namespace Tests.Events
{
    public record ReserveStock
    (
        DateTime timestamp,
        CustomerCheckout customerCheckout,
        IList<CartItem> items,
        int instanceId
    );
}

