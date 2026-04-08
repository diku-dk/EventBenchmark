using System;
using System.Collections.Concurrent;
using Common.Entities;
using Common.Events;

namespace Monitor;

public class OrderActor
{
    private BlockingCollection<PayloadObject> inputMailbox;
    private BlockingCollection<PayloadObject> paymentMailbox;
    
    public OrderActor(BlockingCollection<PayloadObject> inputMailbox, BlockingCollection<PayloadObject> paymentMailbox)
    {
        this.inputMailbox = inputMailbox;
        this.paymentMailbox = paymentMailbox;
    }

    public void Run()
    {
        while (true)
        {
            PayloadObject payload = inputMailbox.Take();
            var message = payload.message_type;
            try
            {
                if (message == Constants.StockConfirmed)
                {
                    var stockConfirmation = (StockConfirmed) payload.payload;
                    var productValue = 0.0;
                    var shipmentValue = 0.0;
                    foreach (var item in stockConfirmation.items)
                    {
                        productValue += item.UnitPrice * item.Quantity;
                        shipmentValue += item.FreightValue;
                    }
                    
                    var toBill = productValue +  shipmentValue;
                    var issuedInvoice = new InvoiceIssued();
                    var newPayLoadObject = new PayloadObject("issue_invoice", issuedInvoice, null);
                    paymentMailbox.Add(newPayLoadObject);
                    // todo logging
                }
                else if (message == Constants.ShipmentNotification)
                {
                    // might not be necessary
                }
                else if (message == Constants.PaymentConfirmed)
                {
                    
                }
                else if (message == Constants.PaymentFailed)
                {
                    
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
        }
    }
}