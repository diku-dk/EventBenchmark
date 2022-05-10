using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Client.UseCases.eShop.TransactionInput;

namespace Client.UseCases.eShop.Transactions
{
    /**
     * This class models a user interacting with the webshop
     * Adding several items accross a ttime span
     * And later checking out the order
     */
    public class Checkout 
    {

        private readonly CheckoutTransactionInput input;
        private readonly HttpClient client;

        // do we have an average timespan between requests?
        public bool Waitable { get; set; }


        public Checkout(HttpClient client, CheckoutTransactionInput input)
        {
            this.input = input;
            this.client = client;
            this.Waitable = true;
        }

        public async Task<HttpResponseMessage> Run(int userId, List<int> itemIds, List<int> itemQuantity)// only parameter not shared across input
        {

            // TODO adjust https://github.com/dotnet-architecture/eShopOnContainers/blob/59805331cd225fc876b9fc6eef3b0d82fda6bda1/src/Web/WebMVC/Infrastructure/API.cs#L17

            Task[] listWaitAddCart = new Task[itemIds.Count];

            for (int i = 0; i < itemIds.Count; i++)
            {
                int itemId = itemIds[i];

                int qty = itemQuantity[i];

                // https://github.com/dotnet-architecture/eShopOnContainers/blob/de90e6e198969eba8bb0a2590f87935aa06cd6ae/src/Web/WebMVC/Controllers/TestController.cs#L3
                var payload = new TestPayload()
                {
                    CatalogItemId = itemId,
                    Quantity = qty,
                    BasketId = userId.ToString()
                };

                var content = new StringContent(JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");

                listWaitAddCart[i] = client.PostAsync(input.CartUrl, content);
                if (Waitable) await Task.Delay(new TimeSpan(1000));
            }

            // wait for all
            Task.WaitAll(listWaitAddCart);

            if (Waitable)
            {
                await Task.Delay(new TimeSpan(10000));
            }

            // now checkout
            return await client.PostAsync(input.CartUrl, null);

        }

        class TestPayload
        {
            public int CatalogItemId { get; set; }

            public string BasketId { get; set; }

            public int Quantity { get; set; }
        }

    }
}
