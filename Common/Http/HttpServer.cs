using System;
using System.Net;
using System.Threading.Tasks;
using Common.Infra;

namespace Common.Http
{
    /**
     * Other solution: https://github.com/beetlex-io/FastHttpApi
     */
    public class HttpServer : Stoppable
    {
        public readonly HttpListener listener;

        public readonly bool blocking;

        public readonly string url;

        private readonly IHttpClientRequestHandler handler;

        public HttpServer(IHttpClientRequestHandler handler, string url = "http://127.0.0.1:8001/", bool blocking = true) : base()
        {
            this.handler = handler;
            this.url = url;
            this.blocking = blocking;
            this.listener = new HttpListener();
        }

        /**
         * There are several ways and techniques to process incoming requests using HttpListener.
         * Some alternatives below:
         * a. https://stackoverflow.com/questions/9034721/handling-multiple-requests-with-c-sharp-httplistener
         * b. https://github.com/JamesDunne/aardwolf/blob/master/Aardwolf/HttpAsyncHost.cs#L107
         * ChatGPT can also provide many. Just start with "How to write a high-performance HTTP server in the programming language C#?"
         * Here we provide two simple ones:
         * (i) Synchronous (blocking) call to listener to get the request object. One request at a time. Can be ok for ingesting data
         * but might perform poorly when triggering workflows that hit many actors.
         * (ii) Relies on the system thread pool to queue new incoming requests. Due to this design choice, when reaching the 
         * maximum number of requests to queue, some requests might be rejected. it is up to the client to adjust itself to
         * the rate being processed by this server, e.g., through using some throttling mechanism.
         */
        private async Task HandleIncomingConnections()
        {
            if (blocking)
            {
                while (this.IsRunning())
                {
                    HttpListenerContext ctx = listener.GetContext();
                    handler.Handle(ctx);
                }
            }
            else
            {
                while (this.IsRunning())
                {

                    HttpListenerContext ctx = await listener.GetContextAsync();

                    _ = Task.Run(() =>
                    {
                        handler.Handle(ctx);
                    });

                }
            }
        }

        public void Run()
        {
            listener.Prefixes.Add(url);
            listener.Start();
            Console.WriteLine("Listening to connections in {0}", url);
            Task task = HandleIncomingConnections();
            task.GetAwaiter().GetResult();
            listener.Close();
        }

    }
}

