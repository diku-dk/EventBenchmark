using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Common.Configuration;
using Common.Customer;
using Common.YCSB;
using GrainInterfaces.Workers;
using Orleans;

namespace Grains.Workers
{
    public class CustomerWorker : Grain, ICustomerWorker
    {

        private static HttpClient client = new HttpClient();

        private static readonly string httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        private static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }

        private readonly Random random = new Random();

        private CustomerConfiguration config;

        private Status status;

        private NumberGenerator keyGenerator;

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
            this.config = metadataService.GetCustomerConfiguration();
            this.keyGenerator = this.config.keyDistribution == Distribution.UNIFORM ?
                new UniformLongGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value) : 
                new ZipfianGenerator(this.config.keyRange.Start.Value, this.config.keyRange.End.Value);
        }

        public async Task Run()
        {
            
            if(this.config.delayBeforeStart > 0) {
                await Task.Delay(this.config.delayBeforeStart);
            }

            int numberOfKeysToBrowse = random.Next(1, this.config.maxNumberKeysToBrowse + 1);

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
            int numberOfKeysToCheckout = random.Next(0, Math.Min( numberOfKeysToBrowse, this.config.maxNumberKeysToAddToCart) + 1);

            long customerId = this.GetPrimaryKeyLong();

            // browsing
            int idx = 0;
            foreach (var entry in keyToQtyMap)
            {
                responses[idx] = await Task.Run( async () =>
                {
                    int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                    try
                    {
                        HttpResponseMessage response = await client.GetAsync(productUrl + entry.Key);
                        await Task.Delay(delay);

                        // is this customer checking out?
                        // decide whether should add to cart now, right after browsing (it helps spreading the requests)
                        if (numberOfKeysToCheckout > 0) // && random.Next(0, 2) > 0
                        {
                            // mark this product as already added to cart
                            keyToQtyMap[entry.Key] = random.Next( this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value + 1);

                            // add to cart
                            string payload = this.config.BuildCartItemPayloadFunc(entry.Key, keyToQtyMap[entry.Key]);
                            try
                            {
                                response = await client.PutAsync(cartUrl + "/" + customerId, BuildPayload(payload));
                                numberOfKeysToCheckout--;
                            } catch (HttpRequestException) { }
                        }
                        return response;
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine("Exception Message: {0}", e.Message);
                        return new HttpResponseMessage(e.StatusCode.HasValue ? e.StatusCode.Value : HttpStatusCode.InternalServerError);
                    }
                        
                    // return;
                });
                idx++;
            }

            // adding to cart the remaining products, if necessary
            /*
            if(numberOfKeysToCheckout > 0)
            {
                foreach (var entry in keyToQtyMap)
                {
                    if (entry.Value > 0) continue;

                    keyToQtyMap[entry.Key] = random.Next(this.config.minMaxQtyRange.Start.Value, this.config.minMaxQtyRange.End.Value + 1);

                    string payload = this.config.BuildCartItemPayloadFunc(entry.Key, keyToQtyMap[entry.Key]);
                    try
                    {
                        await client.PutAsync(cartUrl, BuildPayload(payload));
                        numberOfKeysToCheckout--;
                        int delay = this.random.Next(this.config.delayBetweenRequestsRange.Start.Value, this.config.delayBetweenRequestsRange.End.Value + 1);
                        await Task.Delay(delay);
                    }
                    catch (HttpRequestException) { }

                    if (numberOfKeysToCheckout == 0) break;
                }
            }
            */

            // define whether client should send a checkout request
            if(random.Next(0, 11) > 7) // 8,9,10. 30% if checking out
            {
                await Task.Run(() =>
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, cartUrl + "/" + customerId);
                    message.Content = BuildPayload("");
                    client.Send(message);
                });
            }

            TimeSpan.FromSeconds(5);

            return;
        }


        // make these come from orleans streams
        public Task ReactToAbandonedCart()
        {
            return Task.CompletedTask;
        }


        public Task ReactToPaymentDenied()
        {
            // random. insert a new payment type or cancel the order
            return Task.CompletedTask;
        }

        public Task ReactToOutOfStock()
        {
            return Task.CompletedTask;
        }
    }
}

