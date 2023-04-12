using System;
namespace Common.Scenario.Entity
{
    /**
     * Entity not present in olist original data set
     * Thus, the basket item entity is derived from
     * the needs to process the order.
     * This could include the freight value...
     */
    public class BasketItem
    {
        public long ProductId { get; set; }
        public long SellerId { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal OldUnitPrice { get; set; }
        public decimal FreightValue { get; set; }
        public int Quantity { get; set; }
        public bool unavailable;
    }
}

