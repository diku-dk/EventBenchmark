using System;
using Common.Scenario.Entity;
using System.Collections.Generic;

namespace Common.State
{
    public enum Status
    {
        OPEN,
        CHECKOUT_SENT,
        PRODUCT_DIVERGENCE
    };

    public class CartState
    {
        public Status status;
        public readonly IDictionary<long, BasketItem> items;

        public CartState()
        {
            this.status = Status.OPEN;
            this.items = new Dictionary<long, BasketItem>();
        }
    }
}

