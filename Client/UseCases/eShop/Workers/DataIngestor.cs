
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text.Json;
using System.Threading.Tasks;
using Common.Entities.eShop;

namespace Client.UseCases.eShop.Workers
{

    public class DataIngestor
    {

        private readonly HttpClient httpClient;

        private readonly Queue<KeyValuePair<string, HttpContent>> PendingRequests;

        private int _retries;

        public DataIngestor(HttpClient httpClient)
        {
            this.httpClient = httpClient;
            this.PendingRequests = new();
            this._retries = 1;
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

                       HttpResponseMessage resp = await httpClient.PostAsync(url, payload);

                       if (resp.IsSuccessStatusCode) {
                           return Task.FromResult(resp);
                       }

                       // how to correctly deal with service failures or bottlenecks in the microservice?
                       // we need a strategy to wait and queue
                       // https://docs.microsoft.com/en-us/dotnet/api/system.threading.tasks.taskscheduler?view=net-6.0
                       // TODO better to use a holistic task scheduler...

                       // TODO do we have others? are these correct?
                       if (resp.StatusCode == System.Net.HttpStatusCode.RequestTimeout || resp.StatusCode == System.Net.HttpStatusCode.ServiceUnavailable ||
                                resp.StatusCode == System.Net.HttpStatusCode.TooManyRequests)
                       {

                           Random random = new Random();

                           for (int j = 0; j < _retries; j++)
                           {
                               await Task.Delay(new TimeSpan(random.Next()));
                               resp = await httpClient.PostAsync(url, payload);
                               if (resp.IsSuccessStatusCode)
                               {
                                   return Task.FromResult(resp);
                               }

                           }

                           // queue then...
                           PendingRequests.Enqueue(new KeyValuePair<string, HttpContent>(url, payload));

                       }

                       return Task.CompletedTask;

                   });
            }

            Task.WaitAll(taskArray);

            int pendingCount = PendingRequests.Count;

            // now check whether we have pending requests...
            // this is very rustic way to deal with failures in the target service
            if (pendingCount > 0)
            {

                taskArray = new Task[pendingCount];

                Random random = new Random();

                int i = 0;

                while (PendingRequests.Count > 0)
                {

                    taskArray[i] = Task.Run(async () =>
                    {

                        await Task.Delay(new TimeSpan(random.Next()));
                        var task = PendingRequests.Dequeue();
                        return await httpClient.PostAsync(task.Key, task.Value);

                    });
                }

            }

            Task.WaitAll(taskArray);
            // TODO log the failed pending tasks

        }

        private StringContent BuildPayload(CatalogItem item)
        {

            var payload = new CatalogItemSimple
            {
                Id = item.Id,
                Name = item.Name,
                Price = item.Price,
                PictureUri = null
            };

            return new StringContent(JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");

        }
    }
}
