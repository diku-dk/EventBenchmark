using System;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Client.UseCases.eShop.TransactionInput;
using Common.YCSB;

namespace Client.UseCases.eShop.Transactions
{
    public class PriceUpdate : ITransaction
    {

        private readonly NumberGenerator numberGenerator;
        private readonly PriceUpdateTransactionInput input;
        private readonly HttpClient client;

        private readonly TimeSpan timeSpan;
        private readonly bool Waitable;

        public PriceUpdate(NumberGenerator numberGenerator, PriceUpdateTransactionInput input)
        {
            this.numberGenerator = numberGenerator;
            this.input = input;
            this.client = new HttpClient();
            this.Waitable = false;
        }

        public PriceUpdate(NumberGenerator numberGenerator, PriceUpdateTransactionInput input, TimeSpan timeSpan) : this(numberGenerator, input)
        {
            this.timeSpan = timeSpan;
            this.Waitable = true;
        }


        public Task run()
        {




            throw new NotImplementedException();
        }



    }
}
