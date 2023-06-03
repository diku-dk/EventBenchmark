using System;
using System.Collections.Generic;
using System.Text;

namespace Common.Entity
{
    public class Cart
    {
        // no longer identified within an actor. so it requires an id
        public long customerId { get; set; }

        public CartStatus status { get; set; } = CartStatus.OPEN;

        public IDictionary<long, CartItem> items { get; set; } = new Dictionary<long, CartItem>();

        public DateTime? createdAt { get; set; }

        public string instanceId { get; set; } = "";

        // to return
        public List<ProductStatus>? divergencies { get; set; }

        // for dapr
        public Cart() { }

        public Cart(long customerId) { this.customerId = customerId; }

        public override string ToString()
        {
            return new StringBuilder().Append("CustomerId : ").Append(customerId).ToString();
        }

    }
}

