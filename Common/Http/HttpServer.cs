using System;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common.Infra;

namespace Common.Http
{
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

        private async Task HandleIncomingConnections()
        {
            if (blocking)
            {
                while (this.IsRunning())
                {
                    HttpListenerContext ctx = await listener.GetContextAsync();
                    handler.Handle(ctx);
                }
            }
            else
            {
                while (this.IsRunning())
                {

                    // looks like another way to do it is with continuewith:
                    // https://github.com/JamesDunne/aardwolf/blob/master/Aardwolf/HttpAsyncHost.cs#L107
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

