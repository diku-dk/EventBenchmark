How to test the driver?

Let's break down in each worker.

Customer worker:
We can have a basic test scenario at first, with a single customer

a.The customer requests its own customer info from a API
  So we need to setup a dumb HTTP server to provide such data

b. The customer requests products
   In the HTTP handler we need to provide product info

c. Set checkout probability to 100% to avoid incomplete browsing process

d. Subscribe to receive the status of the customer worker.
   The status at the end must be IDLE

