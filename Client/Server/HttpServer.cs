using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Client.Infra;
using Newtonsoft.Json;

namespace Client.Server
{
	public class HttpServer : Stoppable
	{
        public readonly HttpListener listener;

        public readonly bool blocking;

        public readonly string url = "http://127.0.0.1:8001/";

        public HttpServer(bool blocking = true) : base()
        {
            this.blocking = true;
            this.listener = new HttpListener();
        }

        private static void HandleClientConnection(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            StreamReader stream = new StreamReader(req.InputStream);
            string x = stream.ReadToEnd();

            Console.WriteLine("==== NEW REQUEST =====");
            Console.WriteLine(req.Url.ToString());
            Console.WriteLine(req.HttpMethod);
            Console.WriteLine(req.UserHostName);
            // Console.WriteLine(req.UserAgent);
            Console.WriteLine(x);
            Console.WriteLine("==== END OF REQUEST =====\n");

            // return a basic product json to test the driver
            // byte[] data = Encoding.UTF8.GetBytes("{ productId: 1, quantity: 1 }");

            var model = new{ productId = 1, quantity = 1 };

            byte[] data = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(model));

            resp.ContentType = "application/json";
            // resp.ContentEncoding = Encoding.UTF8;
            resp.ContentLength64 = data.Length;
            resp.StatusCode = 200;
            using System.IO.Stream output = resp.OutputStream;
            output.Write(data, 0, data.Length);
            output.Close();

            resp.Close();
        }

        private async Task HandleIncomingConnections()
        {
            if (blocking)
            {
                while (this.IsRunning())
                {
                    HttpListenerContext ctx = await listener.GetContextAsync();
                    HandleClientConnection(ctx);
                }
            }
            else
            {
                while (this.IsRunning())
                {
                    HttpListenerContext ctx = await listener.GetContextAsync();

                    _ = Task.Run(() =>
                    {
                        HandleClientConnection(ctx);
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

