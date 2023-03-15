using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
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
using Orleans.Concurrency;
using Orleans.Streams;

namespace Grains.Workers
{
    public sealed class CustomerWorker : Grain, ICustomerWorker
    {

        private readonly Random random = new Random();

        private CustomerConfiguration config;

        // private Status status;

        private NumberGenerator keyGenerator;

        private IAsyncStream<Event> stream;

        private long customerId;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        public async override Task OnActivateAsync()
        {
            await base.OnActivateAsync();
            // this.status = Status.NEW;
            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);
            this.config = await metadataService.RetriveCustomerConfig();
            this.keyGenerator = this.config.keyDistribution == Distribution.UNIFORM ?
                new UniformLongGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value) : 
                new ZipfianGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value);

            var streamProvider = this.GetStreamProvider(this.config.streamProvider);
            this.customerId = this.GetPrimaryKeyLong();
            this.stream = streamProvider.GetStream<Event>(this.config.streamId, customerId.ToString());
            // await this.stream.SubscribeAsync<string>( ProcessEventAsync );
            await stream.SubscribeAsync<Event>( ProcessEventAsync );
        }

        public async Task Run()
        {
            Console.WriteLine("Customer {0} started!", this.customerId);

            if(this.config.delayBeforeStart > 0) {
                Console.WriteLine("Customer {0} delay before start: {1}", this.customerId, this.config.delayBeforeStart);
                await Task.Delay(this.config.delayBeforeStart);
            } else
            {
                Console.WriteLine("Customer {0} NO delay before start!", this.customerId);
            }

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

            Console.WriteLine("Customer {0}, number of keys to browse {1}", customerId, numberOfKeysToBrowse);

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
                if(i < numberOfKeysToBrowse-1) sb.Append(", ");
            }

            Console.WriteLine("Customer {0} defined the keys to browse {1}", this.customerId,  sb.ToString());

            string productUrl = this.config.urls["product"];
            string cartUrl = this.config.urls["cart"];

            HttpResponseMessage[] responses = new HttpResponseMessage[numberOfKeysToBrowse];

            // should we also model this behavior?
            int numberOfKeysToCheckout =  
                random.Next(0, Math.Min( numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart));


            Console.WriteLine("Customer {0} will start browsing", this.customerId);

            // browsing
            int idx = 0;
            foreach (KeyValuePair<long,int> entry in keyToQtyMap)
            {
                responses[idx] = await Task.Run( async () =>
                {

                    Console.WriteLine("Customer {0} Task {1}", this.customerId, idx);

                    int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                    try
                    {
                        HttpResponseMessage response = await HttpUtils.client.GetAsync(productUrl + entry.Key);

                        await Task.Delay(delay);

                        // is this customer checking out?
                        // decide whether should add to cart now, right after browsing (it helps spreading the requests)

                        if (numberOfKeysToCheckout > 0)
                        {
                            // mark this product as already added to cart
                            keyToQtyMap[entry.Key] = random.Next( this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value);

                            // add to cart
                            string payload = BuildCartItemPayloadFunc(response.Content.ToString(), keyToQtyMap[entry.Key]);
                            try
                            {
                                response = await HttpUtils.client.PutAsync(cartUrl + "/" + customerId, HttpUtils.BuildPayload(payload));
                            } catch (HttpRequestException e) {
                                Console.WriteLine("Exception Message: {0} Url {1} Key {2}", e.Message, productUrl, entry.Key);
                            } finally
                            {
                                // signaling anyway to avoid hanging forever
                                numberOfKeysToCheckout--;
                            }
                        }
                        return response;
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine("Exception Message: {0}", e.Message);
                        return new HttpResponseMessage(e.StatusCode.HasValue ? e.StatusCode.Value : HttpStatusCode.InternalServerError);
                    }
                   
                });
                idx++;
            }

            Console.WriteLine("Customer " + customerId + " finished browsing!");

            // define whether client should send a checkout request
            if (this.config.checkoutDistribution[random.Next(0, this.config.checkoutDistribution.Length)] == 1)
            {
                Console.WriteLine("Customer " + customerId + " decided to send a checkout!");
                await Task.Run(() => {
                    // checkout must be a url!!!!
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/checkout" + customerId);
                    message.Content = HttpUtils.BuildPayload("");
                    HttpUtils.client.Send(message);
                });
            } else
            {
                Console.WriteLine("Customer " + customerId + " decided not to send a checkout!");
            }

            Console.WriteLine("Customer {0} finished!", this.customerId);

            return;
        }

        private async Task ProcessEventAsync(Event data, StreamSequenceToken token)
        {
            // for each event, forward to the respective handler
            switch (data.topic)
            {
                case "abandoned-cart" : { await this.ReactToAbandonedCart(data.payload); break; }
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
            dynamic obj = JsonConvert.DeserializeObject<ExpandoObject>(productPayload);
            obj.Values.quantity = quantity;
            return JsonConvert.SerializeObject(obj);
        }
    }
}

