using System;
using System.Collections.Concurrent;
using Common.Entities;
using Common.Events;

namespace Monitor;

public class OrderActor
{
    private BlockingCollection<PayloadObject> inputMailbox;
    
    public OrderActor(BlockingCollection<PayloadObject> inputMailbox)
    {
        this.inputMailbox = inputMailbox;
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
                    var newPayLoadObject = new PayloadObject(Constants.InvoiceIssued, issuedInvoice, payload.mailboxes);
                    var paymentMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackStock);
                    paymentMailbox.Add(newPayLoadObject);
                    // todo logging
                }
                else if (message == Constants.ShipmentNotification)
                {
                    // might not be necessary
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