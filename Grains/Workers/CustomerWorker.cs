using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Dynamic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Customer;
using Common.Http;
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

        private Status status;

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
            this.status = Status.NEW;
            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);
            this.config = metadataService.RetriveCustomerConfig();
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
            
            if(this.config.delayBeforeStart > 0) {
                await Task.Delay(this.config.delayBeforeStart);
            }

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse);

            // this dct must be numberOfKeysToCheckout
            ConcurrentDictionary<long, int> keyToQtyMap = new ConcurrentDictionary<long, int>(numberOfKeysToBrowse, numberOfKeysToBrowse);

            long value;
            for (int i = 0; i < numberOfKeysToBrowse; i++)
            {
                value = this.keyGenerator.NextValue();
                while (keyToQtyMap.ContainsKey(value))
                {
                    value = this.keyGenerator.NextValue();
                }
                keyToQtyMap[value] = 0;
            }

            string productUrl = this.config.urls["product"];
            string cartUrl = this.config.urls["cart"];

            HttpResponseMessage[] responses = new HttpResponseMessage[numberOfKeysToBrowse];

            // TODO should we also model this behavior?
            CountdownEvent numberOfKeysToCheckout =  new CountdownEvent(
                random.Next(0, Math.Min( numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart)) );

            // browsing
            int idx = 0;
            foreach (var entry in keyToQtyMap)
            {
                responses[idx] = await Task.Run( async () =>
                {
                    int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                    try
                    {
                        HttpResponseMessage response = await HttpUtils.client.GetAsync(productUrl + entry.Key);
                        await Task.Delay(delay);

                        // is this customer checking out?
                        // decide whether should add to cart now, right after browsing (it helps spreading the requests)

                        if (numberOfKeysToCheckout.CurrentCount > 0) // && random.Next(0, 2) > 0
                        {
                            // mark this product as already added to cart
                            keyToQtyMap[entry.Key] = random.Next( this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value);

                            // add to cart
                            string payload = BuildCartItemPayloadFunc(response.Content.ToString(), keyToQtyMap[entry.Key]);
                            try
                            {
                                response = await HttpUtils.client.PutAsync(cartUrl + "/" + customerId, HttpUtils.BuildPayload(payload));
                                numberOfKeysToCheckout.Signal();
                            } catch (HttpRequestException e) {
                                Console.WriteLine("Exception Message: {0} Url {1} Key {2}", e.Message, productUrl, entry.Key);
                            } finally
                            {
                                // signaling anyway to avoid hanging forever?
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

            // define whether client should send a checkout request
            if(this.config.checkoutDistribution[random.Next(0, this.config.checkoutDistribution.Length)] == 1)
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

