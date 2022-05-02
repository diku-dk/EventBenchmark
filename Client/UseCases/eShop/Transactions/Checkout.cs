using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
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
    public class Checkout : ITransaction
    {

        private readonly NumberGenerator numberGenerator;
        private readonly CheckoutTransactionInput input;
        private readonly HttpClient client;

        // do we have an average timespan between requests?
        private readonly TimeSpan timeSpan;
        private readonly bool Waitable;

        public Checkout(NumberGenerator numberGenerator, CheckoutTransactionInput input)
        {
            this.numberGenerator = numberGenerator;
            this.input = input;
            this.client = new HttpClient();
            this.Waitable = false;
        }

        public Checkout(NumberGenerator numberGenerator, CheckoutTransactionInput input, TimeSpan timeSpan)
                    : this(numberGenerator, input)
        {
            this.timeSpan = timeSpan;
            this.Waitable = true;
        }

        public async Task run()
        {


            // TODo adjust https://github.com/dotnet-architecture/eShopOnContainers/blob/59805331cd225fc876b9fc6eef3b0d82fda6bda1/src/Web/WebMVC/Infrastructure/API.cs#L17

            // default random
            var random = new Random();

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
                string userId = input.UserId;

                // TODO build payload
                HttpContent payload = null;

                listWaitAddCart[i] = client.PostAsync(input.CartUrl, payload);
                if (Waitable) Thread.Sleep(timeSpan);
            }

            // wait for all
            Task.WaitAll(listWaitAddCart);

            if (Waitable) Thread.Sleep(timeSpan);

            

            // now checkout
            HttpResponseMessage response = await client.PostAsync(input.CartUrl, null);

            return;

        }

    }
}
