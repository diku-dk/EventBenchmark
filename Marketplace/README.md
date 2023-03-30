# Marketplace by Orleans
======================================

The port of the marketplace application to Orleans.

## Why Orleans?
Orleans is a notable framework for building distributed applications.
By abstracting complex distributed systems problems, such as dealing with failures, management of distributed objects, and scalability, it allows developers to focus on writing business logic.
By offering virtual actors as the programming model, developers model fine-grained objects that encapsulate state and associated behavior.
These characteristics make Orleans a good candidate for building microservice-based applications.

## Design Principles
We follow some design principles:
- a. We aim at representing real-world entities, such as a customer cart, as finer-grained as possible. That means a 1:1 mapping of application entity to actor.
- b. (Continuing from a.) However, modeling every application entity as an actor sometimes entails an overhead to the application.
     An example is a product or order. There may be thousands (or millions) of them and although representing each of them as an actor may
     not be considered an abuse of abstraction, it might lead to significant increase in context switches because only a portion of them
     will be allowed to be resident in memory at a time (remember that actor code is executed by threads!)
     Thus, to save costs associated to context switches, some entities might be co-located in an actor.
- c. We aim to separate compute and storage whenever possible. To better understand our reasoning, it is worthy to list four (non-exhaustive) categories of actors we consider in this work:
     Data Actors.      Actors that primarily store and serve data. These exhibit a passive behavior, meaning they respond to data access requests from other actors but do not actively participate in transactional workflows. These actors are usually not data-partitioned, managing all the possible entities it represents.
     Stateful Actors.  Actors that encapsulate state and corresponding behavior. These actively participate in workflows, spanning computations in other actors. These fit well with the traditional definition of actors.
     Stateless Actors. Actors that represent a short-lived, non-repeatable task. These are spawned by an actor to support an external task (e.g., communication with an external API) or multiple single-grained tasks that represent a slice of a big task (e.g., shipment of items from different locations in an order). 
     Proxy Actors.     Actors that, due to the partitioning of data across actors, are responsible for assembling the result of many possible tasks that touch different partitions (in our case, actors)

Hybrid actor deployments may also be employed. For instance, a proxy actor can take advantage of many data (where each represnet a data partition) or stateless (where ech represent a fine-grained task) actors.

## Actor Design

Stateful here means it is not a stateless actors.

### Cart
Cart is modeled as a stateful actor because it requires managing the cart state associated behavior of a single customer.
Because customers can only have one cart at a time, carts can be reused across different customer sessions.
In other words, whenever a customer finish an order, if the customer initiates a new session (e.g., adding items to a new cart),
the same cart actor can be reactivated.
In other words, one cart per customer.

The cart actor must store the history of all carts?
What if a customer starts a new order before the previous order has been completed (state is in flux...)?
Multipe carts belonging to same carts, how to enforce the constraint a customer only has a single cart active at a time?

Constraint:
I cart is already sent to checkout, a new cart can be open.

Serve as a user-facing actor. Low-latency. Potentially could have one cart actor for several customers, but caer is short-lived, session-based, different from a product or stock, that benefits from being resident in memory and provides data across customers sessions.

### Stock and Product
Stock and Product are stateful actors partitioned by the product entity identifier.
This design allows for a better distribution of the workload across actors to avoid overhead on accessing the possible many stock and product entities. 

### Order
Order is a stateful actor partitioned by the order identifier.
Another possibility would be partitioning by customer identifier, but that would lead to skewed distribution of work and data, since some customers are more active than others.
Having a partition scheme by order identifier allows a more seamless distribution of work across actors.

### Customer and Seller
Customer is stateful actor partioned customer entity identifier.
Seller is stateful actor partioned seller entity identifier.

-- Customer and Seller are data actors, meaning they hold all customer and seller data, respectively.

### Payment
Payment is a stateful actor partitioned by order identifier.

It receives a payment requets and coordinate with an external service. It is necessary to store data to ensure idempotency (in case of failures, has this external request been made?).
Many payments may occur concurrently.
Payment should be non-reentrant but idempotent.

Add external request to the driver. the payment sucess rate is controlled by the driver.

### Shipment
Shipment is a stateful actors also partitioned by order identifier.
For now, we create a delivery order for each shipment and we complete the checkout process right away.

## Actor APIs

APIs are supposed to being requested only by clients (Other actors should not call them). The benchmark driver workers are an example.

The list of APIs may increase in the future due to new requirements.

Public APIs for external clients.

These are called by driver workers (customers, sellers, and delivery)

Cart
+ AddProduct(BasketItem item)
+ Checkout()

Product
+ GetProduct(long productId);
+ DeleteProduct(long productId);
+ UpdateProductPrice(long productId, decimal newPrice);

Shipment
+ UpdateDeliveryStatus()

These are called by the ingestion process in driver

Product
+ AddProducts

Stock
+ AddStockItems

Customer
+ AddCustomers

Seller
+ AddSellers

Private APIs for internal grains (enabling the transactions)

Order
- ProcessOrder
- CancelOrder ... in case payment fails

Payment
- ProcessPayment

Shipment
- ProcessShipment

Stock
- ReserveProduct(productId)
- CheckoutReservation(productId)
- CancelReservation(productId)

## Transactional Workflows

- checkout: cart, order <-> stock, payment, shipment
- update price: product, cart, customer (if there are customer cart active)
- delete product: product, stock | cart

if does not need a reply, better to use strem abstraction. fits better. eg. checkout
if need a reply, better fit for rpc (also need timeout...). eg order and stock

but to start, implement all with rpc calls

Order actor receives a checkout event and must coordinate with several stock actors because the customer order contains items from many different sellers. How do we proceed?
We asynchronously contact all actors and check whether they have reserved the items?

Orleans impose a timeout on grain calls.
Even though some methods can be interleaved (by having reentrancy enabled), the greater the number of requests, more likely timeouts will be raised by Orleans.

This becomes prohibitive in scenarios where it is necessary to chain actor calls across any different actors, often leading to a domino effect (many timeouts on involved actors).

The escape from this challenge, workflows are enabled via Orleans Streams.

Checkout
Starts with a client call to Actor API Checkout()