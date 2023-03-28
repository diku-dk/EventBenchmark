using System;
namespace Common.Scenario.Entity
{
    public class BasketItem
    {
        public long ProductId { get; set; }
        public string ProductName { get; set; }
        public decimal UnitPrice { get; set; }
        public decimal OldUnitPrice { get; set; }
        public int Quantity { get; set; }
        // public string PictureUrl { get; set; }
    }
}

