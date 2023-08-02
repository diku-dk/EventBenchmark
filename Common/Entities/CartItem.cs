namespace Common.Entities
{
    public record CartItem
    (
       int SellerId,
       int ProductId,
       string ProductName,
       float UnitPrice,
       float FreightValue,
       int Quantity,
       float[] Vouchers
    );
}

