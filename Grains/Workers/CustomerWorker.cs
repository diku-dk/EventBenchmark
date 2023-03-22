using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Http;
using Common.Scenario.Customer;
using Common.Streaming;
using Common.YCSB;
using GrainInterfaces.Scenario;
using GrainInterfaces.Workers;
using Newtonsoft.Json;
using Orleans;
using Orleans.Streams;

namespace Grains.Workers
{
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {

        private readonly Random random = new Random();

        private CustomerConfiguration config;

        private NumberGenerator keyGenerator;

        private IStreamProvider streamProvider;

        private IAsyncStream<Event> stream;

        private long customerId;

        private Status status;

        private string productUrl;

        private string cartUrl;

        private enum Status
        {
            NEW,
            BROWSING,
            CHECKOUT_SENT,
            CHECKOUT_NOT_SENT,
            REACT_OUT_OF_STOCK,
            REACT_FAILED_PAYMENT,
            REACT_ABANDONED_CART
        }

        public async override Task OnActivateAsync()
        {
            this.customerId = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.stream = streamProvider.GetStream<Event>(StreamingConfiguration.CustomerReactStreamId, this.customerId.ToString());
            var subscriptionHandles = await stream.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(ProcessEventAsync);
                }
            }
            await stream.SubscribeAsync<Event>(ProcessEventAsync);

            var workloadStream = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, this.customerId.ToString());
            var subscriptionHandles_ = await workloadStream.GetAllSubscriptionHandles();
            if (subscriptionHandles_.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles_)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }
            await workloadStream.SubscribeAsync<int>(Run);
        }

        public async Task Init()
        {
            this.status = Status.NEW;
            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);
            this.config = await metadataService.RetrieveCustomerConfig();
            this.keyGenerator = this.config.keyDistribution == Distribution.UNIFORM ?
                new UniformLongGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value) :
                new ZipfianGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value);
            this.productUrl = this.config.urls["products"];
            this.cartUrl = this.config.urls["carts"];
            return;
        }

        private async Task Run(int obj, StreamSequenceToken token)
        {
            Console.WriteLine("Customer {0} started!", this.customerId);

            if (this.config.delayBeforeStart > 0)
            {
                Console.WriteLine("Customer {0} delay before start: {1}", this.customerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            }
            else
            {
                Console.WriteLine("Customer {0} NO delay before start!", this.customerId);
            }

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            Console.WriteLine("Customer {0} has this number of keys to browse: {1}", customerId, numberOfKeysToBrowse);

            // this dct must be numberOfKeysToCheckout
            Dictionary<long, int> keyToQtyMap = new Dictionary<long, int>(numberOfKeysToBrowse);

            StringBuilder sb = new StringBuilder();
            long value;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                value = this.keyGenerator.NextValue();
                while (keyToQtyMap.ContainsKey(value))
                {
                    value = this.keyGenerator.NextValue();
                }

                keyToQtyMap.Add(value, 0);
                sb.Append(value);
                if (i < numberOfKeysToBrowse - 1) sb.Append(", ");
            }

            Console.WriteLine("Customer {0} defined the keys to browse: {1}", this.customerId, sb.ToString());

            HttpResponseMessage[] responses = new HttpResponseMessage[numberOfKeysToBrowse];

            // should we also model this behavior?
            int numberOfKeysToCheckout =
                random.Next(0, Math.Min(numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart));

            Console.WriteLine("Customer {0} will start browsing", this.customerId);

            // browsing
            int idx = 0;
            foreach (KeyValuePair<long, int> entry in keyToQtyMap)
            {
                responses[idx] = await Task.Run(async () =>
                {

                    Console.WriteLine("Customer {0} Task {1}", this.customerId, idx+1);

                    int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                    try
                    {
                        HttpResponseMessage response = await HttpUtils.client.GetAsync(productUrl + "/" + entry.Key);

                        await Task.Delay(delay);

                        if (numberOfKeysToCheckout > 0)
                        {
                            // mark this product as already added to cart
                            keyToQtyMap[entry.Key] = random.Next(this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value);

                            // add to cart
                            if(response.Content.Headers.ContentLength == 0)
                            {
                                Console.WriteLine("Response content for product {0} is empty! {1}", entry.Key);
                                return new HttpResponseMessage(HttpStatusCode.InternalServerError);
                            }

                            var productRet = await response.Content.ReadAsStringAsync();
                            string payload = BuildCartItemPayloadFunc(productRet, keyToQtyMap[entry.Key]);
                            try
                            {
                                response = await HttpUtils.client.PutAsync(cartUrl + "/" + customerId, HttpUtils.BuildPayload(payload));
                            }
                            catch (HttpRequestException e)
                            {
                                Console.WriteLine("Exception Message: {0} Url {1} Key {2}", e.Message, productUrl, entry.Key);
                            }
                            finally
                            {
                                // signaling anyway to avoid hanging forever
                                numberOfKeysToCheckout--;
                            }
                        }
                        return response;
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine("HttpRequestException: {0}", e.Message);
                        return new HttpResponseMessage(e.StatusCode.HasValue ? e.StatusCode.Value : HttpStatusCode.InternalServerError);
                    } catch(Exception e_)
                    {
                        Console.WriteLine("Exception: {0}", e_.Message);
                        return new HttpResponseMessage(HttpStatusCode.InternalServerError);
                    }

                });
                idx++;
            }

            Console.WriteLine("Customer " + customerId + " finished browsing!");

            // define whether client should send a checkout request
            if (this.config.checkoutDistribution[random.Next(0, this.config.checkoutDistribution.Length)] == 1)
            {
                Console.WriteLine("Customer " + customerId + " decided to send a checkout!");
                await Task.Run(() =>
                {
                    // checkout must be a url!!!!
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/" + customerId + "/checkout");
                    HttpUtils.client.Send(message);
                });
                this.status = Status.CHECKOUT_SENT;
            }
            else
            {
                this.status = Status.CHECKOUT_NOT_SENT;
                Console.WriteLine("Customer " + customerId + " decided not to send a checkout!");
            }

            return;
        }

        private async Task ProcessEventAsync(Event data, StreamSequenceToken token)
        {
            // for each event, forward to the respective handler
            switch (data.topic)
            {
                case "abandoned-cart": { await this.ReactToAbandonedCart(data.payload); break; }
                case "payment-rejected": { await this.ReactToPaymentRejected(data.payload); break; }
                case "out-of-stock": { await this.ReactToOutOfStock(data.payload); break; }
                default: { Console.WriteLine("Topic: " + data.topic + " has no associated reaction in customer grain " + customerId); break; }
            }
            return;
        }

        private Task ReactToAbandonedCart(string abandonedCartEvent)
        {
            // TODO given a probability, can checkout or not
            return Task.CompletedTask;
        }

        private Task ReactToPaymentRejected(string paymentDeniedEvent)
        {
            // random. insert a new payment type or cancel the order
            return Task.CompletedTask;
        }

        private Task ReactToOutOfStock(string outOfStockEvent)
        {
            return Task.CompletedTask;
        }

        // is this customer-based?
        private static string BuildCartItemPayloadFunc(string productPayload, int quantity)
        {
            // parse into json, add quantity attribute, envelop again
            var obj = JsonConvert.DeserializeObject<ExpandoObject>(productPayload) as IDictionary<string, Object>;
            obj["quantity"] = quantity;
            return JsonConvert.SerializeObject(obj);
        }

    }
}