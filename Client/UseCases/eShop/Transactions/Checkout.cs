using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Client.UseCases.eShop.TransactionInput;
using Common.YCSB;

namespace Client.UseCases.eShop.Transactions
{
    /**
     * This class models a user interacting with the webshop
     * Adding several items accross a ttime span
     * And later checking out the order
     */
    public class Checkout 
    {

        private readonly NumberGenerator numberGenerator;
        private readonly CheckoutTransactionInput input;
        private readonly HttpClient client;

        // do we have an average timespan between requests?
        public bool Waitable { get; set; }

        // default random, instance not thread safe, so require creating a new instance
        private Random random;

        public Checkout(NumberGenerator numberGenerator, CheckoutTransactionInput input)
        {
            this.numberGenerator = numberGenerator;
            this.input = input;
            this.client = new HttpClient();
            this.Waitable = true;
            this.random = new Random();
        }

        public async Task<HttpResponseMessage> Run(string userId)// only parameter not shared across input
        {

            // TODO adjust https://github.com/dotnet-architecture/eShopOnContainers/blob/59805331cd225fc876b9fc6eef3b0d82fda6bda1/src/Web/WebMVC/Infrastructure/API.cs#L17

            // define number of items in the cart
            long numberItems = random.Next(input.MinNumItems, input.MaxNumItems);

            Task[] listWaitAddCart = new Task[numberItems];

            // keep added items to avoid repetition
            Dictionary<int, string> usedItemIds = new Dictionary<int, string>();

            for (int i = 0; i < numberItems; i++)
            {
                int itemId = (int)numberGenerator.NextValue();

                while(usedItemIds[itemId] != null)
                {
                    itemId = (int)numberGenerator.NextValue();
                }

                usedItemIds[itemId] = "";

                int qty = random.Next(input.MinItemQty, input.MaxItemQty);

                // TODO build payload
                HttpContent payload = null;

                listWaitAddCart[i] = client.PostAsync(input.CartUrl, payload);
                if (Waitable) await Task.Delay(new TimeSpan(random.Next(1000,10000)));
            }

            // wait for all
            Task.WaitAll(listWaitAddCart);

            if (Waitable)
            {
                await Task.Delay(new TimeSpan(random.Next(1000, 10000)));
            }

            // now checkout
            return await client.PostAsync(input.CartUrl, null);

        }

    }
}
