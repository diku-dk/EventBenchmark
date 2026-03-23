using System;
using System.Collections.Concurrent;
using Common.Entities;

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
    private BlockingCollection<PayloadObject> outputMailbox;
    private CartActorConfig config;
    private Cart cart;

    // options if static, can go to constructor, otherwise just create volatile fields
    public CartActor(BlockingCollection<PayloadObject> inputMailbox, BlockingCollection<PayloadObject> outputMailbox, CartActorConfig config)
	{
		this.inputMailbox = inputMailbox;
		this.outputMailbox = outputMailbox;
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
				if (message == "checkout")
				{
					
				} 
				else if (message == "add_item")
				{
					AddItem((CartItem) payload.payload);
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


			// if the config allows, output it
			this.outputMailbox.Add(payload);
		}

	}

}


