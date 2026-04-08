namespace Monitor;

public static class Constants
{
    public const string Checkout = "checkout";
    public const string AddItem = "add_item";
    
    public const string AddStock = "add_stock";
    public const string ReserveInventory = "reserve_inventory";
    public const string StockConfirmed = "stock_confirmed";
    public const string StockReservationFailed = "stock_rejected";
    
    public const string ShipmentNotification = "shipment_notification";
    public const string PaymentConfirmed = "payment_confirmed";
    public const string PaymentFailed = "payment_failed";
    
    public const string IssueInvoice = "issue_invoice";
    
    public const string CallBackOrder = "Orders";
    public const string CallBackCostumer = "Costumer";
}