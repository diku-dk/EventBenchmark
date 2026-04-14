using System;
using System.Collections.Concurrent;
using Common.Events;

namespace Monitor;

public class PaymentActor
{
    private BlockingCollection<PayloadObject> inputMailbox;
    
    public PaymentActor(BlockingCollection<PayloadObject> inputMailbox)
    {
        this.inputMailbox =  inputMailbox;
    }

    private bool InvoiceProcessed()
    {
        return true;
    }

    public void Run()
    {
        while (true)
        {
            PayloadObject payload = inputMailbox.Take();
            var message = payload.message_type;

            try
            {
                if (message == Constants.InvoiceIssued)
                {
                    // compute if invoice can be processed
                    var invoice = (InvoiceIssued) payload.payload; 
                    if (InvoiceProcessed())
                    {
                        var valid = new PaymentConfirmed(invoice.customer, invoice.orderId, invoice.totalInvoice, invoice.items, invoice.issueDate, invoice.instanceId);
                        CallbackManager.AddCallBackAddress(payload.mailboxes, Constants.CallBackPayment, inputMailbox);
                        var newPayLoadObject = new PayloadObject(Constants.PaymentConfirmed, valid, payload.mailboxes);
                        var costumerOutputMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackCostumer);
                        costumerOutputMailbox.Add(newPayLoadObject);
                        var stockOutputMailbox =  CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackStock);
                        stockOutputMailbox.Add(newPayLoadObject);
                        var shipmentOutputMailbox =  CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackShipment);
                        shipmentOutputMailbox.Add(newPayLoadObject);
                        // todo logging
                    }
                    else
                    {
                        var invalid = new PaymentFailed("Failed", invoice.customer, invoice.orderId, invoice.items, invoice.totalInvoice, invoice.instanceId);
                        var newPayLoadObject = new PayloadObject(Constants.PaymentFailed, invalid, payload.mailboxes);
                        var costumerOutputMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackCostumer);
                        costumerOutputMailbox.Add(newPayLoadObject);
                        var stockOutputMailbox =  CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackStock);
                        stockOutputMailbox.Add(newPayLoadObject);
                        // todo logging
                    }
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