using System;
namespace Common.Entities
{
    public record CartItem
    (
       long SellerId,
       long ProductId,
       string ProductName,
       decimal UnitPrice,
       decimal FreightValue,
       int Quantity,
       decimal[] Vouchers
    );
}

