using Moq;
using Moq.Protected;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;

namespace Tests.Driver;

public class MicroBench
{

    static int numberOfTasks = 8;
    static int numberOfThreads = 5;
    static int numberOfIterations = 100;

    [Fact]
    public void Test()
    {
        var handlerAsync = new Mock<HttpMessageHandler>();
        
        handlerAsync.Protected()
                                 .Setup<Task<HttpResponseMessage>>("SendAsync",
                                    ItExpr.IsAny<HttpRequestMessage>(),
                                    ItExpr.IsAny<CancellationToken>())
                                 //.Callback(() => Task.Delay(5000))
                                 .ReturnsAsync(new HttpResponseMessage
                                 {
                                     StatusCode = HttpStatusCode.OK
                                 }, TimeSpan.FromMilliseconds(5000));
                               

        var handlerSync = new Mock<HttpMessageHandler>();
        handlerSync.Protected()
                         .Setup<HttpResponseMessage>("SendSync",
                            ItExpr.IsAny<HttpRequestMessage>(),
                            ItExpr.IsAny<CancellationToken>())
                         .Callback(() => System.Threading.Thread.Sleep(1000))
                         .Returns(new HttpResponseMessage
                         {
                             StatusCode = HttpStatusCode.OK
                         });

        var httpClientSync = new HttpClient(handlerSync.Object)
        {
            // BaseAddress = new Uri("http://test.com/")
        };

        var httpClientAsync = new HttpClient(handlerAsync.Object)
        {
            // BaseAddress = new Uri("http://test.com/")
        };

        var requestQueue = new ConcurrentQueue<HttpRequestMessage>();

        FillQueue(requestQueue, numberOfIterations);



    }

    static void FillQueue(ConcurrentQueue<HttpRequestMessage> requestQueue, int numberOfIterations)
    {
        // Enqueue your HTTP requests
        for (int i = 0; i < numberOfIterations; i++)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, $"https://example.com/api/resource{i}");
            requestQueue.Enqueue(request);
        }
    }

    static async Task MeasureWithTaskPerRequestSync(int numberOfTasks, int numberOfIterations, HttpClient httpClient)
    {
        List<Task> tasks = new List<Task>(numberOfIterations);
        
        var stopwatch = new Stopwatch();
        stopwatch.Start();
 
        for (int j = 0; j < numberOfIterations; j++)
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, "https://example.com/api/resource");
            tasks.Add(Task.Run( () =>
                {
                    httpClient.Send(request);
                }
            ));
        }

        await Task.WhenAll(tasks);

        stopwatch.Stop();
        Console.WriteLine($"TaskPerRequestSync Execution Time: {stopwatch.ElapsedMilliseconds} ms");

    }

    static async Task MeasureWithTaskPerRequestAsync(int numberOfTasks, int numberOfIterations, HttpClient httpClient)
    {
        List<Task> tasks = new List<Task>(numberOfIterations);
        var stopwatch = new Stopwatch();
        stopwatch.Start();

        for (int j = 0; j < numberOfIterations; j++)
        {
            HttpRequestMessage request = new HttpRequestMessage(HttpMethod.Get, "https://example.com/api/resource");
            tasks.Add(httpClient.SendAsync(request));
        }

        await Task.WhenAll(tasks);

        stopwatch.Stop();
        Console.WriteLine($"TaskPerRequestAsync Execution Time: {stopwatch.ElapsedMilliseconds} ms");

    }

    static async Task MeasureWithFixedTasksAsync(HttpClient httpClient, ConcurrentQueue<HttpRequestMessage> requestQueue)
    {

        CancellationTokenSource source = new CancellationTokenSource();

        // Start worker tasks
        var workerTasks = new List<Task>();

        var stopwatch = new Stopwatch();
        stopwatch.Start();

        for (int i = 0; i < numberOfTasks; i++)
        {
            workerTasks.Add(Task.Run(() => WorkerAsync(httpClient, requestQueue, source.Token)));
        }
        // Wait for all workers to complete
        await Task.WhenAll(workerTasks);

        stopwatch.Stop();
        Console.WriteLine($"FixedTasksAsync Execution Time: {stopwatch.ElapsedMilliseconds} ms");
    }

    static async Task WorkerAsync(HttpClient httpClient, ConcurrentQueue<HttpRequestMessage> requestQueue, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            if (requestQueue.TryDequeue(out HttpRequestMessage request))
            {
                await httpClient.SendAsync(request);
            }
        }
    }

    static async Task MeasureWithFixedTasksSync(HttpClient httpClient, ConcurrentQueue<HttpRequestMessage> requestQueue)
    {

        CancellationTokenSource source = new CancellationTokenSource();

        // Start worker tasks
        var workerTasks = new List<Task>();

        var stopwatch = new Stopwatch();
        stopwatch.Start();

        for (int i = 0; i < numberOfTasks; i++)
        {
            workerTasks.Add(Task.Run(() => WorkerSync(httpClient, requestQueue, source.Token)));
        }
        // Wait for all workers to complete
        await Task.WhenAll(workerTasks);

        stopwatch.Stop();
        Console.WriteLine($"FixedTasksSync Execution Time: {stopwatch.ElapsedMilliseconds} ms");
    }

    static async Task WorkerSync(HttpClient httpClient, ConcurrentQueue<HttpRequestMessage> requestQueue, CancellationToken token)
    {
        while (!token.IsCancellationRequested)
        {
            if (requestQueue.TryDequeue(out HttpRequestMessage request))
            {
                httpClient.Send(request);
            }
        }
    }

}

