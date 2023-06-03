using Common.Infra;
using System.Net;
using System.IO;
using Orleans;
using System;
using Common.Entity;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common.Event;
using System.Text;
using Marketplace.Interfaces;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Marketplace.Infra
{
	public class HttpHandler : IHttpClientRequestHandler
    {

        // orleans client
        private readonly IClusterClient orleansClient;

        private readonly ILogger _logger;

        public HttpHandler(IClusterClient orleansClient)
        {
            this.orleansClient = orleansClient;
            using var loggerFactory = LoggerFactory.Create(loggingBuilder => loggingBuilder
                                                    .SetMinimumLevel(LogLevel.Warning)
                                                    .AddConsole());
            this._logger = loggerFactory.CreateLogger("default");
        }

        /**
         * ChatGPT: "[...] method is marked as async void, which is 
         * usually not recommended, but it is acceptable in this case 
         * because the method is called from within an asynchronous delegate."
         */
        public async void Handle(HttpListenerContext ctx)
        {

            this._logger.LogWarning("[HttpHandler] {0} {1} request: segments | absolute path | absolute uri: {2} | {3} | {4}",
                              DateTime.Now.Millisecond,  ctx.Request.HttpMethod,
                            ctx.Request.Url.Segments, ctx.Request.Url.AbsolutePath, ctx.Request.Url.AbsoluteUri);

            string resource;
            if (ctx.Request.Url.Segments[1].Contains('/'))
                resource = ctx.Request.Url.Segments[1].Split('/')[0];
            else
                resource = ctx.Request.Url.Segments[1];
            this._logger.LogWarning("Resource is {0}", resource);

            // map url to respective actor
            switch (resource)
            {
                case "carts":
                    {
                        await HandleCartRequestAsync(ctx);
                        return;
                    }
                case "products":
                    {
                        await HandleProductRequestAsync(ctx);
                        return;
                    }
                case "sellers":
                    {
                        await HandleSellerRequestAsync(ctx);
                        return;
                    }
                case "customers":
                    {
                        await HandleCustomerRequestAsync(ctx);
                        return;
                    }
                case "stock_items":
                    {
                        await HandleStockItemRequestAsync(ctx);
                        return;
                    }
                case "shipments":
                    {
                        await HandleShipmentRequestAsync(ctx);
                        return;
                    }
                default:
                    {
                        var resp = ctx.Response;
                        resp.StatusCode = 404;
                        resp.Close();
                        _logger.LogWarning("Failed to process the request in Http Handler.");
                        return;
                    }
            }
        }

        private async Task HandleCartRequestAsync(HttpListenerContext ctx)
        {

            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.Segments[2];
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
                        string id = ctx.Request.Url.Segments[2];
                        id = id.Replace("/","");
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
                        string id = ctx.Request.Url.Segments[2];
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<CartItem>(x);
                        await orleansClient.GetGrain<ICartActor>(Convert.ToInt64(id)).AddProduct(obj);
                        resp.StatusCode = 200;
                        resp.Close();
                        break;
                    }
                case "PATCH":
                    {
                        string id = ctx.Request.Url.Segments[2];
                        await orleansClient.GetGrain<ICartActor>(Convert.ToInt64(id)).ClearCart();
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
                        string id = ctx.Request.Url.Segments[2];
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
                        string id = ctx.Request.Url.Segments[2];
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

                    // check if there query is presented
                    if (req.Url.AbsoluteUri.Contains('?'))
                    {
                        // seller_id=15
                        string[] parts = req.Url.AbsoluteUri.Split('=');
                        long sellerId = Convert.ToInt64(parts[1]);

                        var products = await orleansClient.GetGrain<IProductActor>(0).GetProducts(sellerId);

                        var payload = JsonConvert.SerializeObject(products);
                        byte[] data = Encoding.UTF8.GetBytes(payload);
                        resp.ContentType = "application/json";
                        resp.ContentLength64 = data.Length;
                        resp.StatusCode = 200;
                        using Stream output = resp.OutputStream;
                        output.Write(data, 0, data.Length);
                        output.Close();
                        resp.Close();
                    }
                    else
                    {

                        string id = ctx.Request.Url.Segments[2];
                        Product product = await orleansClient.GetGrain<IProductActor>(0).GetProduct(Convert.ToInt64(id));
                        var payload = JsonConvert.SerializeObject(product);
                        byte[] data = Encoding.UTF8.GetBytes(payload);
                        resp.ContentType = "application/json";
                        resp.ContentLength64 = data.Length;
                        resp.StatusCode = 200;
                        using Stream output = resp.OutputStream;
                        output.Write(data, 0, data.Length);
                        output.Close();
                        resp.Close();
                            
                    }
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
                case "DELETE":
                {
                    string id = ctx.Request.Url.Segments[2];
                    await orleansClient.GetGrain<ISellerActor>(0).DeleteProduct(Convert.ToInt64(id));
                    resp.StatusCode = 200;
                    resp.Close();
                    break;
                }
                case "PATCH":
                {
                    // update prices, reach to seller actor
                    StreamReader stream = new StreamReader(req.InputStream);
                    string x = stream.ReadToEnd();
                    List<Product> products = JsonConvert.DeserializeObject<List<Product>>(x);
                    // all products expected to be from the same seller id
                    long sellerId = products[0].seller_id;
                    await orleansClient.GetGrain<ISellerActor>(sellerId).UpdatePrices(products);
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

            _logger.LogWarning("Handling customer request");

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.Segments[2];
                        var obj = await orleansClient.GetGrain<ICustomerActor>(0).GetCustomer(Convert.ToInt64(id));
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

        }

        private async Task HandleShipmentRequestAsync(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            _logger.LogWarning("Handling shipment request");

            switch (req.HttpMethod)
            {
                case "GET":
                    {
                        string id = ctx.Request.Url.Segments[2];
                        var obj = await orleansClient.GetGrain<IShipmentActor>(0).GetPendingPackagesBySeller(Convert.ToInt64(id));
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
                case "PATCH":
                    {
                        StreamReader stream = new StreamReader(req.InputStream);
                        string x = stream.ReadToEnd();
                        var obj = JsonConvert.DeserializeObject<Customer>(x);
                        await orleansClient.GetGrain<IShipmentActor>(0).UpdateShipment();
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

    }
}

