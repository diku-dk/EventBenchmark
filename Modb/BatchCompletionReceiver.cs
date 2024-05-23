using Common.Infra;
using Common.Workload;
using System.Net;

namespace Modb;

/**
 * https://learn.microsoft.com/en-us/dotnet/api/system.net.httplistener?view=net-8.0
 */
public sealed class BatchCompletionReceiver: StoppableImpl
{
    private readonly HttpListener listener;

    private readonly int port;

    private int tidCount;

    public BatchCompletionReceiver(int port) : base()
    {
        this.port = port;
        this.listener = new HttpListener();
        this.tidCount = 0;
    }

    public void Run()
    {
        this.listener.Prefixes.Add($"http://localhost:{this.port}/");
        this.listener.Prefixes.Add($"http://127.0.0.1:{this.port}/");
        this.listener.Prefixes.Add($"http://+:{this.port}/");
        this.listener.Prefixes.Add($"http://*:{this.port}/");
        this.listener.Start();

        Console.WriteLine($"Listening to connections in port {this.port}");
      
        while (this.IsRunning())
        {
            HttpListenerContext ctx = this.listener.GetContext();
            // spawn a worker to process request
            Task.Run(()=> HandleRequest(ctx)); 
        }
       
        this.listener.Close();
    }

    private void HandleRequest(HttpListenerContext ctx)
    {
        // Console.WriteLine("New result!!!");

        StreamReader stream = new StreamReader(ctx.Request.InputStream);
        string contentStr = stream.ReadToEnd();
        HttpListenerResponse response = ctx.Response;
        response.ContentLength64 = 0;

        if (string.IsNullOrEmpty(contentStr)) {                            
            Console.WriteLine("Error on reading batch completion signal from MODB: content is null!");
            response.StatusCode = 500;
            response.OutputStream.Close();
        } else {
        
            response.StatusCode = 200;
            response.OutputStream.Close();

            string[] split = contentStr.Split('-');
                
            _ = int.TryParse(split[0], out int init);
            _ = int.TryParse(split[1], out int end);

            Console.WriteLine("Adding results "+init+" to "+end);

            int idx = init;
            while(idx <= end)
            {
                // Console.WriteLine("Adding result "+init);
                Shared.ResultQueue.Writer.TryWrite(Shared.ITEM);
                idx++;
                
            }
            int sumToAdd = end-init+1;
            // Console.WriteLine("Sum to add: "+sumToAdd);
         
            Interlocked.Add(ref this.tidCount, sumToAdd);
        }

    }

    public int LastTID()
    {
        return Volatile.Read(ref this.tidCount);
    }

    public void RestartTID()
    {
        Volatile.Write(ref this.tidCount, 0);
    }

}

