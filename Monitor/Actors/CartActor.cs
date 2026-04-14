using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Common.Entities;
using Common.Events;
using Common.Requests;

namespace Monitor;

// we are simulating the behavior of an external entity (in an external entity)

// we can model 1:1 mapping (this means fine grained config and execution/concurrency)
// but also large stack/memory pressure and context switches and resource consumption
// we can model N:1 mapping. we can model the behavior of many customer carts through a single cart actor
// event loop will receive checkout requests from different customers
// this config can be transparent to this actor class (i.e., abstract this away)
public class CartActor
{

	// behaviors
	// out of order, delays, anything that we want to simulate
	// distributions, failure probability

	private BlockingCollection<PayloadObject> inputMailbox;
    private CartActorConfig config;
    private Cart cart;
    private bool checkedOut = false; 

    // options if static, can go to constructor, otherwise just create volatile fields
    public CartActor(BlockingCollection<PayloadObject> inputMailbox, CartActorConfig config)
	{
		this.inputMailbox = inputMailbox;
		this.config = config;
		cart = new Cart();
	}
    
	private void AddItem(CartItem cartItem)
	{
		if (cartItem.Quantity <= 0)
		{
			throw new Exception("Negative quantity cannot be added!");
		}
		
		if (cart.status == CartStatus.CHECKOUT_SENT)
		{
			throw new Exception("Cart for customer " + cart.customerId + " already sent for checkout.");
		}
		
		cart.items.Add(cartItem);
	}

	public void Run()
	{
		// event loop
		while (true)
		{
			// wait for a customer checkout
			PayloadObject payload = inputMailbox.Take();
			// TODO do things
			var message = payload.message_type;
			try
			{
				if (message == Constants.Checkout)
				{
					if (checkedOut)
					{

					}
					else
					{
						checkedOut = true;
						ReserveInventory reverse_inventory = new ReserveInventory(DateTime.UtcNow, new CustomerCheckout(), (List<CartItem>) cart.items, cart.instanceId.ToString());
						var stockMailBox = CallbackManager.GetCallBackMailbox(payload.mailboxes, Constants.CallBackStock);
						var newPayload = new PayloadObject(Constants.ReserveInventory, reverse_inventory, payload.mailboxes);
						stockMailBox.Add(newPayload);
						// todo logging
					}
				} 
				else if (message == Constants.AddItem)
				{
					if (checkedOut)
					{
						// todo logging
					}
					else
					{
						AddItem((CartItem) payload.payload);
						// todo logging
					}
					
				}
				else
				{
					throw new Exception("Unknown message type: " + message);
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


