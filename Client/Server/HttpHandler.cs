using System;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Client.Infra;
using Common.Infra;
using Newtonsoft.Json;

namespace Client.Server
{
	public class HttpHandler : IHttpClientRequestHandler
	{

        public void Handle(HttpListenerContext ctx)
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

    }
}

