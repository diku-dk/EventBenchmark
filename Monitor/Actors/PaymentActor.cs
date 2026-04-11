using System;
using System.Collections.Concurrent;
using Common.Events;

namespace Monitor;

public class PaymentActor
{
    private BlockingCollection<PayloadObject> inputMailbox;
    private BlockingCollection<PayloadObject> stockOutputMailbox;
    private BlockingCollection<PayloadObject> shipmentOutputMailbox;
    
    public PaymentActor(
        BlockingCollection<PayloadObject> inputMailbox, 
        BlockingCollection<PayloadObject> stockOutputMailbox,
        BlockingCollection<PayloadObject> shipmentOutputMailbox)
    {
        this.inputMailbox =  inputMailbox;
        this.stockOutputMailbox = stockOutputMailbox;
        this.shipmentOutputMailbox = shipmentOutputMailbox;
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
                    if (InvoiceProcessed())
                    {
                        var valid = new PaymentConfirmed();
                        CallbackManager.AddCallBackAddress(payload.mailboxes, Constants.CallBackPayment, inputMailbox);
                        var newPayLoadObject = new PayloadObject(Constants.PaymentConfirmed, valid, payload.mailboxes);
                        var costumerOutputMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackCostumer);
                        costumerOutputMailbox.Add(newPayLoadObject);
                        stockOutputMailbox.Add(newPayLoadObject);
                        shipmentOutputMailbox.Add(newPayLoadObject);
                        // todo logging
                    }
                    else
                    {
                        var invalid = new PaymentFailed();
                        var newPayLoadObject = new PayloadObject(Constants.PaymentFailed, invalid, payload.mailboxes);
                        var costumerOutputMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackCostumer);
                        costumerOutputMailbox.Add(newPayLoadObject);
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