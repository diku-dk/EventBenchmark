using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.Configuration;
using Client.UseCases.eShop.TransactionInput;
using Client.UseCases.eShop.Transactions;
using Client.UseCases.eShop.Workers;
using Common.Entities.eShop;
using Common.YCSB;

/**
 * 
     1- define the use case OK

     2 - define the transactions and respective percentage   OK

     3 - define the distribution of each transaction    OK

     4 - setup data generation   OK

     5 - setup workers to submit requests   OK

     6 - setup event listeners (rabbit mq) given the config
 * 
 */
namespace Client.UseCases.eShop
{
    public class EShopUseCase : IStoppable
    {

        private readonly IUseCaseConfig Config;

        private readonly CountdownEvent cde;

        public EShopUseCase(IUseCaseConfig Config)
        {
            this.Config = Config;
            this.cde = new CountdownEvent(1);
        }

        // the constructor are expected to be the same..
        // but creating so many classes is not good, i believe i should model as tasks....
        // FIXME https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/task-based-asynchronous-programming
        // automatic thread pool control

        // orleans can used as reliable metadatada and configuration storage, as well as programming abstraction run...
        // some tasks are better off orleans?

        public void Prepare()
        {

            CheckoutTransactionInput checkoutTransactionInput = new CheckoutTransactionInput
            {
                MinNumItems = Constants.MIN_NUM_ITEMS,
                MaxNumItems = Constants.MAX_NUM_ITEMS,
                MinItemQty = Constants.MIN_ITEM_QTY,
                MaxItemQty = Constants.MAX_ITEM_QTY,
                CartUrl = Config.GetUrlMap()["basket"]
            };

            List<ApplicationUser> customers =  DataGenerator.GenerateCustomers(Constants.CUST_PER_DIST);
            List<CatalogItem> items = DataGenerator.GenerateCatalogItems(Constants.NUM_TOTAL_ITEMS);

            HttpClient httpClient = new HttpClient();

            List<string> urlList = new List<string>
            {
                Config.GetUrlMap()["basket"],
                Config.GetUrlMap()["catalog"]
            };


            DataIngestor dataIngestor = new DataIngestor(httpClient);

            // blocking call
            dataIngestor.RunCatalog(Config.GetUrlMap()["catalog"], items);

            // TODO setup event listeners before submitting the transactions



            // now that data is stored, we can start the transacions
            RunTransactions(checkoutTransactionInput);

        }

        private void RunTransactions(CheckoutTransactionInput input)
        {

            Random random = new Random();
            HttpClient client = new HttpClient();

            int userCount = 0;

            int n = Config.GetTransactions().Count;

            // build and run all transaction tasks

            while (!cde.IsSet)
            {

                int k = random.Next(0, n-1);

                switch (Config.GetTransactions()[k])
                {

                    case "Checkout":
                        {

                            NumberGenerator numberGenerator = GetDistribution();

                            // define number of items in the cart
                            long numberItems = random.Next(input.MinNumItems, input.MaxNumItems);

                            // keep added items to avoid repetition
                            Dictionary<int, string> usedItemIds = new Dictionary<int, string>();

                            List<int> itemIds = new();

                            List<int> itemQuantity = new();

                            userCount++;

                            // constraint here: numberItems >= NUM_TOTAL_ITEMS
                            for (int i = 0; i < numberItems; i++)
                            {
                                int itemId = (int)numberGenerator.NextValue();

                                while (usedItemIds[itemId] == "")
                                {
                                    itemId = (int)numberGenerator.NextValue();
                                }

                                usedItemIds[itemId] = "";

                                itemIds.Add(itemId);

                                int qty = random.Next(input.MinItemQty, input.MaxItemQty);

                                itemQuantity.Add(qty);

                            }

                            Checkout checkout = new Checkout(client, input);

                            Task<HttpResponseMessage> res = Task.Run(async () =>
                            {
                                return await checkout.Run(userCount, itemIds, itemQuantity);
                            });

                            // add to concurrent queue and check if error is too many requests, if so, send again later
                            //   this is to maintain the distribution

                            break;
                        }

                }


            }

        }

        private NumberGenerator GetDistribution()
        {
            if( Config.GetDistribution() == Distribution.NORMAL)
            {
                return new UniformLongGenerator(1,Constants.NUM_TOTAL_ITEMS);
            }

            return new ZipfianGenerator(Constants.NUM_TOTAL_ITEMS);
        }


        public void Stop()
        {
            cde.Signal();
        }

    }
}
