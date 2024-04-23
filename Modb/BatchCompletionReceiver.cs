using Common.Infra;
using Common.Workload;
using System.Net;

namespace Modb;

/**
 * https://learn.microsoft.com/en-us/dotnet/api/system.net.httplistener?view=net-8.0
 */
public sealed class BatchCompletionReceiver: StoppableImpl
{
    public readonly HttpListener listener;

    public readonly string url;

    public BatchCompletionReceiver(string url = "http://localhost:9000/") : base()
    {
        this.url = url;
        this.listener = new HttpListener();
    }

    public void Run()
    {
        this.listener.Prefixes.Add(url);
        this.listener.Start();
        Console.WriteLine("Listening to connections in {0}", url);
        while (this.IsRunning())
        {
            HttpListenerContext ctx = this.listener.GetContext();

            // spawn a worker to process request
            _ = Task.Run(() => {
   
                StreamReader stream = new StreamReader(ctx.Request.InputStream);
                string contentStr = stream.ReadToEnd();
                if (string.IsNullOrEmpty(contentStr)) {                            
                    Console.WriteLine("Error on reading batch completion signal from MODB: content is null!");
                }

                HttpListenerResponse response = ctx.Response;
                response.ContentLength64 = 0;
                response.StatusCode = 200;
                response.OutputStream.Close();

                string[] split = contentStr.Split('-');
                
                _ = int.TryParse(split[0], out int init);
                _ = int.TryParse(split[1], out int end);

                Console.WriteLine("Adding results "+init+" to "+end);

                while(init <= end)
                {
                    Shared.ResultQueue.Writer.TryWrite(Shared.ITEM);
                    init++;
                }

            });
            
        }
       
        this.listener.Close();
    }

}

