using Common.Ingestion;
using GrainInterfaces.Ingestion;
using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Grains.Ingestion
{
    /**
     * The unit of parallelism for data ingestion
     */
    [StatelessWorker]
    public class IngestionWorker : Grain, IIngestionWorker
    {

        // https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=net-7.0
        private static readonly HttpClient client = new HttpClient();

        private static readonly String httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public async override Task OnActivateAsync()
        {
            return;
        }

        public async Task Send(IngestionBatch batch)
        {
            List<Task<HttpStatusCode>> responses = RunBatch(batch);
            await Task.WhenAll(responses);
            return;
        }

        public async Task Send(List<IngestionBatch> batches)
        {

            Console.WriteLine("Batches received: "+ batches.Count);

            List<Task<HttpStatusCode>> responses = new List<Task<HttpStatusCode>>();

            foreach(IngestionBatch batch in batches) 
            {
                responses.AddRange(RunBatch(batch));
            }

            Console.WriteLine("All http requests sent");

            await Task.WhenAll(responses);

            Console.WriteLine("All responses received");

            return;

        }

        private static List<Task<HttpStatusCode>> RunBatch(IngestionBatch batch)
        {
            List<Task<HttpStatusCode>> responses = new List<Task<HttpStatusCode>>();
            foreach (string payload in batch.data)
            {
                // https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains
                // https://stackoverflow.com/questions/10343632/httpclient-getasync-never-returns-when-using-await-async
                // https://blog.stephencleary.com/2012/07/dont-block-on-async-code.html
                responses.Add(Task.Factory.StartNew(async () =>
                {
                    try
                    {
                        using HttpResponseMessage response = await client.PostAsync(batch.url, BuildPayload(payload));
                        response.EnsureSuccessStatusCode();
                        Console.WriteLine("Here we are: " + response.StatusCode);
                        return response.StatusCode;
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine("\nException Caught!");
                        Console.WriteLine("Message: {0}", e.Message);
                        return e.StatusCode.Value;
                    }
                }
                ).Unwrap());
            }
            return responses;
        }

        private static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }

    }
}
