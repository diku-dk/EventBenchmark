using System.Collections.Generic;
using System.Net.Http;
using System.Text;

namespace Common.Http
{
    public sealed class HttpUtils
    {

        // https://learn.microsoft.com/en-us/dotnet/api/system.net.http.httpclient?view=net-7.0
        // // HttpClient must be instantiated once per application. See above
        public static readonly HttpClient client = new HttpClient();

        private static readonly string httpJsonContentType = "application/json";

        private static readonly Encoding encoding = Encoding.UTF8;

        public static StringContent BuildPayload(string item)
        {
            return new StringContent(item, encoding, httpJsonContentType);
        }

    }
}