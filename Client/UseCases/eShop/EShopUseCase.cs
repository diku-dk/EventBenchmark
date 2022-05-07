using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.UseCases.eShop.TransactionInput;
using Client.UseCases.eShop.Transactions;
using Client.UseCases.eShop.Workers;
using Common.Entities.eShop;
using Common.YCSB;

/**
 * 
     1- define the use case

     2 - define the transactions and respective percentage

     3 - define the distribution of each transaction

     4 - setup data generation

     5 - setup workers to submit requests

     6 - setup event listeners (rabbit mq) given the config
 * 
 */
namespace Client.UseCases.eShop
{
    public class EShopUseCase : IStoppable
    {

        //private readonly List<Type> TransactionTypes;
        //private List<ITransaction> Transactions;
        // what transactions are involved?
        // what is the data distribution?
        // what is the percentage of each transaction?

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
                CartUrl = "http:/localhost:9000/basket"
            };

            List<ApplicationUser> customers =  DataGenerator.GenerateCustomers(Constants.CUST_PER_DIST);
            List<CatalogItem> items = DataGenerator.GenerateCatalogItems(Constants.NUM_TOTAL_ITEMS);

            HttpClient httpClient = new HttpClient();

            List<string> urlList = new List<string>
            {
                "http:/localhost:9000/basket",
                "http:/localhost:9000/catalog"
            };


            DataIngestor dataIngestor = new DataIngestor(httpClient);

            // blocking call
            dataIngestor.RunCatalog("http:/localhost:9000/catalog", items);

            // TODO setup event listeners before submitting the transactions



            // now that data is stored, we can start the transacions
            RunTransactions(checkoutTransactionInput);

        }

        private void RunTransactions(CheckoutTransactionInput input)
        {

            int n = Config.GetTransactions().Count;

            // build and run all transaction tasks

            while (!cde.IsSet)
            {

                // TODO pick a transaction based on probability
                int i = 0;


                switch (Config.GetTransactions()[i])
                {

                    case "Checkout":
                        {


                            NumberGenerator numberGenerator = new ZipfianGenerator(Constants.NUM_TOTAL_ITEMS);

                            Checkout checkout = new Checkout(numberGenerator, input);

                            Task<HttpResponseMessage> res = Task.Run(async () =>
                            {
                                return await checkout.Run("1");
                            });

                            // add to concurrent queue and check if error is too many requests, if so, send again later
                            //   this is to maintain the distribution

                            break;
                        }





                }



            }

        }


        public void Stop()
        {
            cde.Signal();
        }



    }
}
