using System.Text;

namespace Common.Entities
{
    public class Cart
    {
        // no longer identified within an actor. so it requires an id
        public int customerId { get; set; }

        public CartStatus status { get; set; } = CartStatus.OPEN;

        public IList<CartItem> items { get; set; } = new List<CartItem>();

        public int instanceId { get; set; }

        // to return
        public List<ProductStatus> divergencies { get; set; } = new();

        // for dapr
        public Cart() { }

        public override string ToString()
        {
            return new StringBuilder().Append("CustomerId : ").Append(customerId).ToString();
        }

    }
}

