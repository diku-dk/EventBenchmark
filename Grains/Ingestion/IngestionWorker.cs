using Common.Http;
using Common.Ingestion;
using Common.Ingestion.DTO;
using GrainInterfaces.Ingestion;
using Orleans;
using Orleans.Concurrency;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Grains.Ingestion
{
    /**
     * The unit of parallelism for data ingestion
     * A dispatcher of HTTP POST requests.
     * Why not simply using threads? Because with thread-only approahc there is no
     * simple way to track multiple machines. With grains, Orleans provides multi-machine
     * settings by design.
     * Basically this encapsulates a remote task...
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
            var tasks = RunBatch(batch);
            await Task.WhenAll(tasks);
            return;
        }

        public async Task Send(List<IngestionBatch> batches)
        {

            Console.WriteLine("Batches received: "+ batches.Count);

            List<Task<HttpStatusCode[]>> responses = new();

            foreach(IngestionBatch batch in batches) 
            {
                responses.Add(RunBatch(batch));
            }

            Console.WriteLine("All HTTP requests sent. Time to wait for the responses...");
            
            foreach (var tasks in responses)
            {
                await Task.WhenAll(tasks);
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

        private async Task<HttpStatusCode[]> RunBatch(IngestionBatch batch)
        {
            HttpStatusCode[] responses = new HttpStatusCode[batch.data.Count];
            int idx = 0;
            Random random = new();
            foreach (string payload in batch.data)
            {
                // https://learn.microsoft.com/en-us/dotnet/orleans/grains/external-tasks-and-grains
                // https://stackoverflow.com/questions/10343632/httpclient-getasync-never-returns-when-using-await-async
                // https://blog.stephencleary.com/2012/07/dont-block-on-async-code.html


                // maybe implement mechanism for backpressure! start with concurrent submissions and then 4, 8, 16, if error downgrade to 8, 4, 2, 1 ...
                // actually the unit of backpressure is the number of ingestion workers...
                responses[idx] = await (Task.Run( () =>
                {
                    HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Post, batch.url);
                    message.Content = HttpUtils.BuildPayload(payload);
                    while (true)
                    {
                        try
                        {
                            using HttpResponseMessage response = HttpUtils.client.Send(message);
                            return response.StatusCode;
                        }
                        catch (HttpRequestException e)
                        {
                            // Console.WriteLine("\nException Caught!");
                            Console.WriteLine("Message: {0}", e.Message);
                            Thread.Sleep(random.Next(1,1001)); // spread the several errors that may happen across different workers
                            // return HttpStatusCode.ServiceUnavailable;
                        }
                    }
                }));
                idx++;

            }
            return responses;
        }

    }
}
