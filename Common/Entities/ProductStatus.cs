using System;
namespace Common.Entities
{
    public class ProductStatus
    {
        public long Id { get; set; }
        public ItemStatus Status { get; set; }
        public decimal UnitPrice { get; set; } = 0;
        public decimal OldUnitPrice { get; set; } = 0;
        public int QtyAvailable { get; set; } = 0;

        public ProductStatus(long id, ItemStatus status, decimal price, decimal oldPrice)
        {
            this.Id = id;
            this.Status = status;
            this.UnitPrice = price;
            this.OldUnitPrice = oldPrice;
        }

        public ProductStatus(long id, ItemStatus status)
        {
            this.Id = id;
            this.Status = status;
        }

        public ProductStatus(long id, ItemStatus status, int qtyAvailable)
        {
            this.Id = id;
            this.Status = status;
            this.QtyAvailable = qtyAvailable;
        }

    }
}

