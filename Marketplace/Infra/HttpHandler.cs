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
using Common.Entity;

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

        /**
         * ChatGPT: "[...] method is marked as async void, which is 
         * usually not recommended, but it is acceptable in this case 
         * because the method is called from within an asynchronous delegate."
         */
        public async void Handle(HttpListenerContext ctx)
        {
            // TODO
            // a.make sure all http calls from workers reach here
            // b. init a simple transaction workload (only with customers)
            // c. continue adding new transactions. monitor the system

            // Console.WriteLine("Marketplace Http Handler new event!");
            // map url to respective actor
            switch (ctx.Request.Url.AbsolutePath)
            {
                case "/carts":
                    {
                        await HandleCartRequestAsync(ctx);
                        return;
                    }
                case "/products":
                    {
                        await HandleProductRequestAsync(ctx);
                        return;
                    }
                case "/sellers":
                    {
                        await HandleSellerRequestAsync(ctx);
                        return;
                    }
                case "/customers":
                    {
                        await HandleCustomerRequestAsync(ctx);
                        return;
                    }
                case "/stock_items":
                    {
                        await HandleStockItemRequestAsync(ctx);
                        return;
                    }
            }
            // Console.WriteLine("Marketplace Http Handler not a product event!");
        }

        private async Task HandleCartRequestAsync(HttpListenerContext ctx)
        {

            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        var obj = await orleansClient.GetGrain<ICartActor>(Convert.ToInt64(id)).GetCart();
                        var payload = JsonConvert.SerializeObject(obj);
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
                        string id = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<CustomerCheckout>(x);
                        await orleansClient.GetGrain<ICartActor>(Convert.ToInt64(id)).Checkout(obj);
                        resp.StatusCode = 200;
                        resp.Close();
                        break;
                    }
                case "PUT":
                    {
                        string id = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<BasketItem>(x);
                        await orleansClient.GetGrain<ICartActor>(Convert.ToInt64(id)).AddProduct(obj);
                        resp.StatusCode = 200;
                        resp.Close();
                        break;
                    }
            }
        }

        private async Task HandleStockItemRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        var obj = await orleansClient.GetGrain<IStockActor>(0).GetItem(Convert.ToInt64(id));
                        var payload = JsonConvert.SerializeObject(obj);
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
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<StockItem>(x);
                        await orleansClient.GetGrain<IStockActor>(0).AddItem(obj);
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


        private async Task HandleSellerRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        var obj = await this.orleansClient.GetGrain<ISellerActor>(0).GetSeller();
                        var payload = JsonConvert.SerializeObject(obj);
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
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        // Console.WriteLine("Seller object received from producer: {0}", x);
                        Seller obj = JsonConvert.DeserializeObject<Seller>(x);
                        await this.orleansClient.GetGrain<ISellerActor>(obj.id).Init(obj);
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

        private async Task HandleProductRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                {
                    string productId = ctx.Request.Url.AbsolutePath.Split('/')[1];
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
                    StreamReader stream = new StreamReader(req.InputStream);
                    string x = stream.ReadToEnd();
                    Product product = JsonConvert.DeserializeObject<Product>(x);
                    await orleansClient.GetGrain<IProductActor>(0).AddProduct(product);
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

        private async Task HandleCustomerRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string customerId = ctx.Request.Url.AbsolutePath.Split('/')[1];
                        var obj = await orleansClient.GetGrain<ICustomerActor>(0).GetCustomer(Convert.ToInt64(customerId));
                        var payload = JsonConvert.SerializeObject(obj);
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
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<Customer>(x);
                        await orleansClient.GetGrain<ICustomerActor>(0).AddCustomer(obj);
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

            /*
            if (!pathAndQuery.Contains('?'))
            {
                // Console.WriteLine("Entered POST method!!!");
                StreamReader stream = new StreamReader(req.InputStream);
                string x = stream.ReadToEnd();
                Product product = JsonConvert.DeserializeObject<Product>(x);
                await orleansClient.GetGrain<IProductActor>(0).AddProduct(product);
                // Console.WriteLine("product added successfully!!!");
                resp.StatusCode = 200;
                resp.Close();
                // break;
            }
            else
            {
                string[] split = pathAndQuery.Split('?');
                // long.Parse(split[1])
                Product product = await orleansClient.GetGrain<IProductActor>(0).GetProduct(Convert.ToInt64(split[1]));
            }
            */
        }

	}
}

