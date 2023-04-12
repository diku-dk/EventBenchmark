using Common.Infra;
using System.Net;
using System.IO;
using Orleans;

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

        public void Handle(HttpListenerContext ctx)
        {

            // map url to respective actor
            if (ctx.Request.Url.AbsolutePath.Contains("products"))
            {
                HandleProductRequest(ctx);
                return;
            }

        }

        private void HandleProductRequest(HttpListenerContext ctx)
        {
            HttpListenerRequest req = ctx.Request;
            HttpListenerResponse resp = ctx.Response;

            StreamReader stream = new StreamReader(req.InputStream);
            string x = stream.ReadToEnd();


        }

        private void HandleCustomerRequest(string uri)
        {

        }

	}
}

