using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Client.Infra;

namespace Client.Server
{
	public class HttpServer : Stoppable
	{
        public readonly HttpListener listener;
        public readonly string url = "http://127.0.0.1:8001/";

        public HttpServer() : base()
        {
            this.listener = new HttpListener();
        }

        public async Task HandleIncomingConnections()
        {

            while (IsRunning())
            {
                HttpListenerContext ctx = await listener.GetContextAsync();

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
                byte[] data = Encoding.UTF8.GetBytes("{ productId: 1, quantity: 1 }");
                resp.ContentType = "application/json";
                resp.ContentEncoding = Encoding.UTF8;
                resp.ContentLength64 = data.Length;
                resp.StatusCode = 200;
                await resp.OutputStream.WriteAsync(data, 0, data.Length);

                resp.Close();
            }
        }

        public void Run()
        {
            
           
            listener.Prefixes.Add(url);
            listener.Start();
            Console.WriteLine("Listening for connections on {0}", url);
  
            Task listenTask = HandleIncomingConnections();
            listenTask.GetAwaiter().GetResult();

            listener.Close();
        }

    }
}

