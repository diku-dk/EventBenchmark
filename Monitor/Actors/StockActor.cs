using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Entities;
using Common.Events;

namespace Monitor;

public class StockActor
{
    private Dictionary<int, int> inventory;
    private BlockingCollection<PayloadObject> inputMailbox;
    
    public StockActor(Dictionary<int, int> inventory, BlockingCollection<PayloadObject> inputMailbox)
    {
        this.inputMailbox = inputMailbox;
        this.inventory = inventory;   
    }

    private void AddStock(int productId, int quantity)
    {
        if (inventory.ContainsKey(productId))
        {
            inventory[productId] += quantity;
        }
        else
        {
            inventory.Add(productId, quantity);
        }
    }

    private bool ReserveProducts(List<CartItem> cartItems)
    {
        lock (this)
        {
            foreach (var cartItem in cartItems)
            {
                var qty = cartItem.Quantity;
                var key = cartItem.ProductId;

                if (!inventory.ContainsKey(key))
                {
                    return false;
                }

                if (inventory[key] < qty)
                {
                    return false;
                }
            }

            foreach (var cartItem in cartItems)
            {
                var qty = cartItem.Quantity;
                var key = cartItem.ProductId;
                
                inventory[key] -= qty;
            }
            return true;
        }
    }
    
    public void Run()
    {
        while (true) 
        {
            PayloadObject payload = inputMailbox.Take();
            var message = payload.message_type;

            try
            {
                if (message == Constants.AddStock)
                {
                    var val = (Tuple<int, int>) payload.payload;
                    AddStock(val.Item1, val.Item2);
                    // todo log event
                }
                else if (message == Constants.ReserveInventory)
                {
                    var val = (ReserveInventory) payload.payload;
                    if (ReserveProducts(val.items))
                    {
                        var newPayload = new StockConfirmed();
                        CallbackManager.AddCallBackAddress(payload.mailboxes, Constants.CallBackStock, inputMailbox);
                        PayloadObject newPayloadObject = new PayloadObject(Constants.StockConfirmed, newPayload, payload.mailboxes);
                        var orderOutputMailbox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackStock);
                        orderOutputMailbox.Add(newPayloadObject);
                        // todo logging send E3 stock confirmed
                    }
                    else
                    {
                        var newPayload = new ReserveStockFailed();
                        PayloadObject newPayloadObject = new PayloadObject(Constants.StockReservationFailed, newPayload, payload.mailboxes);
                        var costumerMailbox = payload.mailboxes[Constants.CallBackCostumer];
                        costumerMailbox.Add(newPayloadObject);
                        // todo logging send E4 Stock reservation failed
                    }
                }
                else if (message == Constants.PaymentFailed)
                {
                     var paymentFailed = (PaymentFailed) payload.payload;
                     foreach (var item in paymentFailed.items)
                     {
                         AddStock(item.ProductId, item.Quantity);
                     }
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