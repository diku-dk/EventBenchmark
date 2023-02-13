using Common.Http;
using Common.Ingestion;
using GrainInterfaces.Ingestion;
using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

namespace Grains.Ingestion
{
    /**
     * The unit of parallelism for data ingestion
     * A dispatcher of HTTP POST requests
     */
    [StatelessWorker]
    public class IngestionWorker : Grain, IIngestionWorker
    {

        public async override Task OnActivateAsync()
        {
            Console.WriteLine("Ingestion worker on activate!");
            await base.OnActivateAsync();
            return;
        }

        public async Task Send(IngestionBatch batch)
        {
            Task<HttpStatusCode>[] tasks = RunBatch(batch);
            await Task.WhenAll(tasks);
            /*
            foreach (Task<HttpStatusCode> task in tasks)
            {
                await task;
            }
            */
            return;
        }

        public async Task Send(List<IngestionBatch> batches)
        {

            Console.WriteLine("Batches received: "+ batches.Count);

            List<Task<HttpStatusCode>[]> responses = new List<Task<HttpStatusCode>[]>();

            foreach(IngestionBatch batch in batches) 
            {
                responses.Add(RunBatch(batch));
            }

            Console.WriteLine("All http requests sent");

            foreach (Task<HttpStatusCode>[] tasks in responses)
            {
                await Task.WhenAll(tasks);
                /*
                foreach (Task<HttpStatusCode> task in tasks) 
                {
                    await task; 
                }
                */
            }

            Console.WriteLine("All responses received");

            // TODO check correctness... make get requests looking for some random IDs. also total sql to count total of items ingested
            CheckCorrectness();

            return;
        }

        /*
         * Given PK of each table, sample records to verify correctness of the data
         * Modify the return, from http status to the PK of the record
         * Then make a get with the PK and compare with the submitted payload
         */
        private bool CheckCorrectness()
        {
            return true;
        }

        private Task<HttpStatusCode>[] RunBatch(IngestionBatch batch)
        {
            Task<HttpStatusCode>[] responses = new Task<HttpStatusCode>[batch.data.Count];
            int idx = 0;
            foreach (string payload in batch.data)
            {
                // https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains
                // https://stackoverflow.com/questions/10343632/httpclient-getasync-never-returns-when-using-await-async
                // https://blog.stephencleary.com/2012/07/dont-block-on-async-code.html
                responses[idx] = (Task.Run(() =>
                {
                    try
                    {
                        HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, batch.url);
                        message.Content = HttpUtils.BuildPayload(payload);
                        using HttpResponseMessage response = HttpUtils.client.Send(message);
                        return response.StatusCode;
                    }
                    catch (HttpRequestException e)
                    {
                        Console.WriteLine("\nException Caught!");
                        Console.WriteLine("Message: {0}", e.Message);
                        return HttpStatusCode.ServiceUnavailable;
                    }
                }
                ));

                idx++;

            }
            return responses;
        }

    }
}
