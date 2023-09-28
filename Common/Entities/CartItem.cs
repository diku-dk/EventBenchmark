namespace Common.Entities;

public class CartItem {

    public int SellerId { get; set; }

    public int ProductId { get; set; }

    public string ProductName { get; set; }

    public float UnitPrice { get; set; }

    public float FreightValue { get; set; }

    public int Quantity { get; set; }

    public float Voucher { get; set; }

    public string Version { get; set; }

    public CartItem() { }

    public CartItem(int sellerId, int productId, string productName, float unitPrice, float freightValue, int quantity, float voucher, string version)
    {
        SellerId = sellerId;
        ProductId = productId;
        ProductName = productName;
        UnitPrice = unitPrice;
        FreightValue = freightValue;
        Quantity = quantity;
        Voucher = voucher;
        Version = version;
    }

    public override string ToString()
    {
        return "{ SellerId "+SellerId+" ProductId "+ProductId+" }";
    }
}
