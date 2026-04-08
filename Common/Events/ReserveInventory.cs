using Common.Entities;
using Common.Requests;

namespace Common.Events;

public class ReserveInventory
{
    public DateTime timestamp {get; set; }

    public CustomerCheckout customerCheckout {get; set; }

    public List<CartItem> items {get; set; }

    public string instanceId {get; set; }

    public ReserveInventory(){ }

    public ReserveInventory(DateTime timestamp, CustomerCheckout customerCheckout, List<CartItem> items, string instanceId)
    {
        this.timestamp = timestamp;
        this.customerCheckout = customerCheckout;
        this.items = items;
        this.instanceId = instanceId;
    }

    public override string ToString()
    {
        // todo make mfotl event ReverseInventory(customer, card, items)
        return string.Join(",",items);
    }
}