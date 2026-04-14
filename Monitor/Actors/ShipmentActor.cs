using System;
using System.Collections.Concurrent;
using Common.Entities;
using Common.Events;

namespace Monitor;

public class ShipmentActor
{
    private BlockingCollection<PayloadObject> inputMailbox;
    
    public ShipmentActor(BlockingCollection<PayloadObject> inputMailbox)
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
                if (message == Constants.PaymentConfirmed)
                {
                    var paymentConfirmed = (PaymentConfirmed)payload.payload;
                    var shipmentNotification = new ShipmentNotification(
                        paymentConfirmed.customer.CustomerId, paymentConfirmed.orderId,
                        paymentConfirmed.date, paymentConfirmed.instanceId, ShipmentStatus.delivery_in_progress
                    );
                    var orderCallback = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackOrder);
                    var newPayload = new PayloadObject(Constants.ShipmentNotification, shipmentNotification, payload.mailboxes);
                    orderCallback.Add(newPayload);
                    // todo logging
                } else if (message == Constants.Delivered)
                {
                    var deliveryNotification = new DeliveryNotification();
                    var costumerCallback =
                        CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackCostumer);
                    var newPayload = new PayloadObject(Constants.DeliveryNotification, deliveryNotification, payload.mailboxes);
                    costumerCallback.Add(newPayload);
                    // todo logging
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