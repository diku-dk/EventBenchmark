
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Common.Entities.eShop;

namespace Client.UseCases.eShop.Workers
{

    public class DataIngestor
    {

        private readonly HttpClient httpClient;

        public DataIngestor(HttpClient httpClient)
        {
            this.httpClient = httpClient;
        }

        private void RunCatalogParallel(string url, List<CatalogItem> items)
        {

            int n = items.Count;
            Task<HttpResponseMessage>[] tasks = new Task<HttpResponseMessage>[n];

            // https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/how-to-write-a-simple-parallel-for-loop

            #region Parallel_Loop
            _ = Parallel.For(0, n - 1, i =>
                {
                    HttpContent payload = BuildPayload(items[i]);
                    tasks[i] = httpClient.PostAsync(url, payload);
                });
            #endregion

            Task.WaitAll(tasks);

        }

        /**
         * TPL is the preferred API for writing multi-threaded, asynchronous, and parallel code in .NET.
         * https://docs.microsoft.com/en-us/dotnet/standard/parallel-programming/how-to-create-pre-computed-tasks
         */
        public void RunCatalog(string url, List<CatalogItem> items)
        {
            int n = items.Count;
            Task[] taskArray = new Task[n];
            //Task<HttpResponseMessage>[] taskArray = new Task<HttpResponseMessage>[n];

            for (int i = 0; i < taskArray.Length; i++)
            {
                taskArray[i] = Task.Run( async () =>
                   {
                       HttpContent payload = BuildPayload(items[i]);
                       // return await httpClient.PostAsync(url, payload);

                       HttpResponseMessage resp = await httpClient.PostAsync(url, payload);

                       if (resp.IsSuccessStatusCode) return Task.CompletedTask;

                       Random random = new Random();

                       // continue trying FIXME depending on status code
                       while (!resp.IsSuccessStatusCode) //&& resp.StatusCode != )
                       {
                           
                           await Task.Delay(new TimeSpan(random.Next()));
                           resp = await httpClient.PostAsync(url, payload);

                       }

                       return Task.FromResult(resp);

                   });
            }

            Task.WaitAll(taskArray);

        }

        private HttpContent BuildPayload(CatalogItem item)
        {
            // return HttpContent;
            return null;
        }
    }
}
