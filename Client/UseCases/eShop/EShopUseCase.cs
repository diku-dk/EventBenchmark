using System;
using System.Collections.Generic;
using System.Reflection;

namespace Client.UseCases.eShop
{
    public class EShopUseCase
    {


        private readonly List<Type> TransactionTypes;
        private List<ITransaction> Transactions;
        // what transactions are involved?
        // what is the data distribution?
        // what is the percentage of each transaction?

        private readonly IUseCaseConfig Config;

        public EShopUseCase(IUseCaseConfig Config)
        {
            this.Config = Config;


        }

        public void init()
        {


            // get public constructors
            var ctors = Config.GetTransactions()[0].GetConstructors(BindingFlags.Public);

            // the constructor are expecte to be the same..
            // but creating so many classes is not good, i believe i should model as tasks....

            // invoke the first public constructor with no parameters.
            var obj = ctors[0].Invoke(new object[] { });


            // public Task GenerateData();


        }




    }
}
