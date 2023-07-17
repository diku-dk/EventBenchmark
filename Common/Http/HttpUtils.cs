using System.Text;

namespace Common.Http
{
    public sealed class HttpUtils
    {

        // https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=net-7.0
        // // HttpClient must be instantiated once per application. See above

        // https://stackoverflow.com/questions/19883524/why-is-httpclients-getstringasync-is-unbelivable-slow
        //static HttpClientHandler httpClientHandler = new Func<HttpClientHandler>(() => {
        //    // do it here
        //    var handler = new HttpClientHandler();
        //    handler.UseProxy = false;
        //    handler.Proxy = null;
        //    handler.PreAuthenticate = false;
        //    handler.UseDefaultCredentials = false;
        //    return handler;
        //})();

        //public static readonly HttpClient client = new HttpClient();

        // https://www.stevejgordon.co.uk/using-httpcompletionoption-responseheadersread-to-improve-httpclient-performance-dotnet
        // https://www.stevejgordon.co.uk/httpclient-connection-pooling-in-dotnet-core
        public static readonly HttpClient client = new HttpClient(new SocketsHttpHandler()
        {
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(10),
            PooledConnectionLifetime = TimeSpan.FromMinutes(10),
            UseProxy = false,
            Proxy = null,
            // MaxConnectionsPerServer = 100
        });

        private static readonly string httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }
        
    }
}