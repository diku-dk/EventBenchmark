using MathNet.Numerics.LinearAlgebra;
using System.Text;

namespace Common.Http
{
    public sealed class HttpUtils
    {

        // https://www.stevejgordon.co.uk/using-httpcompletionoption-responseheadersread-to-improve-httpclient-performance-dotnet
        // https://www.stevejgordon.co.uk/httpclient-connection-pooling-in-dotnet-core
        public static readonly HttpClient client = new HttpClient(new SocketsHttpHandler()
        {
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            UseProxy = false,
            Proxy = null
        });

        private static readonly string JsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, JsonContentType);
        }

        public static StringContent BuildPayload(string item, string contentType)
        {
            return new StringContent(item, encoding, contentType);
        }

        /**
        * For StateFun only
        *   used to send http request to StateFun application.
        */
        public static async Task<HttpResponseMessage> SendHttpToStatefun(string url, string contentType, string payLoad)
        {
            var content = HttpUtils.BuildPayload(payLoad);
            content.Headers.ContentType = null; // zero out default content type
            content.Headers.TryAddWithoutValidation("Content-Type", contentType);
            
            HttpResponseMessage response = await client.PostAsync(url, content);    

            return response;
        }
    }
}