using System;
using System.Collections.Concurrent;
using Common.Workload.CustomerWorker;

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

	private BlockingCollection<object> inputMailbox;
    private BlockingCollection<object> outputMailbox;
    private CartActorConfig config;

    // options if static, can go to constructor, otherwise just create volatile fields
    public CartActor(BlockingCollection<object> inputMailbox, BlockingCollection<object> outputMailbox, CartActorConfig config)
	{
		this.inputMailbox = inputMailbox;
		this.outputMailbox = outputMailbox;
		this.config = config;
	}

	// message communication between threads

	// everything related to thread synchronization also applies to this
	// but there are other more beneficial (?) designs

	// mutual exclusion, atomic variables
	// blocking queue.

	public void Run()
	{

		// event loop
		while (true)
		{
			// wait for a customer checkout
			object payload = inputMailbox.Take();

			// TODO do things


			// if the config allows, output it
			this.outputMailbox.Add(payload);
		}

	}

}


