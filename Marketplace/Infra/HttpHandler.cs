using Common.Infra;
using System.Net;
using System.IO;
using Orleans;
using Marketplace.Actor;
using System;
using Common.Scenario.Entity;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Reflection;
using System.Text;
using System.Net.Http;
using Marketplace.Interfaces;

namespace Marketplace.Infra
{
	public class HttpHandler : IHttpClientRequestHandler
    {

        // orleans client
        private readonly IClusterClient orleansClient;

        public HttpHandler(IClusterClient orleansClient)
        {
            this.orleansClient = orleansClient;
        }

        public async void Handle(HttpListenerContext ctx)
        {
            // Console.WriteLine("Marketplace Http Handler new event!");
            // map url to respective actor
            if (ctx.Request.Url.AbsolutePath.Contains("products"))
            {
                // Task task = HandleProductRequestAsync(ctx);
                // task.Wait();

                // can this be performed safely? without losing the context?
                await HandleProductRequestAsync(ctx);
                return;
            }
            // Console.WriteLine("Marketplace Http Handler not a product event!");
        }

        private async Task HandleProductRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                {
                    // Console.WriteLine("Entered get method!!!");

                    string productId = ctx.Request.Url.AbsolutePath.Split('/')[1];

                    // Console.WriteLine("Product id "+productId);

                    Product product = await orleansClient.GetGrain<IProductActor>(0).GetProduct(Convert.ToInt64(productId));

                    var payload = JsonConvert.SerializeObject(product);

                    byte[] data = Encoding.UTF8.GetBytes(payload);

                    resp.ContentType = "application/json";
                    resp.ContentLength64 = data.Length;
                    resp.StatusCode = 200;
                    using Stream output = resp.OutputStream;
                    output.Write(data, 0, data.Length);
                    output.Close();

                    resp.Close();

                    break;
                }
                case "POST":
                {
                    // Console.WriteLine("Entered POST method!!!");
                    StreamReader stream = new StreamReader(req.InputStream);
                    string x = stream.ReadToEnd();
                    Product product = JsonConvert.DeserializeObject<Product>(x);
                    await orleansClient.GetGrain<IProductActor>(0).AddProduct(product);
                    // Console.WriteLine("product added successfully!!!");
                    resp.StatusCode = 200;
                    resp.Close();
                    break;
                }
                default:
                {
                    Console.WriteLine("Entered NO method!!!");
                    break;
                }
            }

            

        }

        private async void HandleCustomerRequest(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            StreamReader stream = new StreamReader(req.InputStream);
            string x = stream.ReadToEnd();

            string pathAndQuery = ctx.Request.Url.PathAndQuery;

            if (pathAndQuery.Contains('?'))
            {

            }
            else
            {
                string[] split = pathAndQuery.Split('?');
                // long.Parse(split[1])
                Product product = await orleansClient.GetGrain<IProductActor>(0).GetProduct(Convert.ToInt64(split[1]));
            }
        }

	}
}

