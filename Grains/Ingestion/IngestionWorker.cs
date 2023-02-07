using Common.Configuration;
using Common.Entities.eShop;
using Common.Http;
using Common.Ingestion;
using GrainInterfaces.Ingestion;
using GrainInterfaces.Workers;
using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Grains.Ingestion
{
    /**
     * The unit of parallelism for data ingestion
     */
    [StatelessWorker]
    public class IngestionWorker : Grain, IIngestionWorker
    {

        private readonly HttpClient client = new HttpClient();

        readonly String httpJsonContentType = "application/json";

        readonly System.Text.Encoding encoding = System.Text.Encoding.UTF8;

        public async override Task OnActivateAsync()
        {
            
            // this.client.
            return;
        }

        public async Task Send(IngestionBatch batch)
        {
            List<Task<HttpResponseMessage>> responses = new List<Task<HttpResponseMessage>>();

            foreach(string payload in batch.data)
            {
                responses.Add(client.PostAsJsonAsync(batch.url, payload) ); // PostAsync BuildPayload( payload ) ) );
            }

            await Task.WhenAll(responses);

            // foreach (Task<HttpResponseMessage> response in responses) await response;

            return;

        }

        public async Task Send(List<IngestionBatch> batches)
        {
            List<Task<HttpResponseMessage>> responses = new List<Task<HttpResponseMessage>>();

            foreach(IngestionBatch batch in batches) 
            { 
                foreach (string payload in batch.data)
                {
                    responses.Add(client.PostAsJsonAsync(batch.url, payload));  // BuildPayload(payload)));
                }
            }

            await Task.WhenAll(responses);

            // foreach (Task<HttpResponseMessage> response in responses) await response;

            return;

        }


        private static StringContent BuildPayload(string item)
        {
            return new StringContent(item, Encoding.UTF8, "application/json");
        }

    }
}
