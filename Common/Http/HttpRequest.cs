using System.Collections.Generic;

namespace Common.Http
{
    public class HttpRequest
    {

        // unique identifier
        public int requestId;

        public string url { get; set; }

        public Dictionary<string, string> payload { get; set; }

        public List<KeyValuePair<string, string>> headers;

    }
}