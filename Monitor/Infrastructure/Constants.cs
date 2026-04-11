using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

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
    
    public const string InvoiceIssued = "invoice_issued";
    
    public const string CallBackOrder = "Orders";
    public const string CallBackCostumer = "Costumer";
    public const string CallBackPayment = "Payment";
}


public class CallbackManager
{
    public static void AddCallBackAddress(Dictionary<String, BlockingCollection<PayloadObject>> mailboxes, String name, BlockingCollection<PayloadObject> mailbox)
    {
        mailboxes.TryAdd(name, mailbox);
    }

    public static BlockingCollection<PayloadObject> GetCallBackMailbox(Dictionary<String, BlockingCollection<PayloadObject>> mailboxes, String name)
    {
        return mailboxes[name];
    }
}