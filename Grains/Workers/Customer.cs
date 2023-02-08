using System;
using System.Net.Http;
using System.Threading.Tasks;
using Common.Customer;
using Common.Ingestion;
using GrainInterfaces.Workers;
using Orleans;

namespace Grains.Workers
{
    public class Customer : Grain, ICustomer
    {
        private readonly HttpClient client = new HttpClient();

        private CustomerConfiguration config;

        private Status status;

        private enum Status
        {
            NEW,
            IN_PROGRESS,
            FINISHED
        }

        public async override Task OnActivateAsync()
        {
            this.status = Status.NEW;
            return;
        }

        public async Task Run(CustomerConfiguration config)
        {
            this.config = config;

            await Task.Delay(TimeSpan.FromSeconds(5));

            return;
        }

        // make these come from orleans streams

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

