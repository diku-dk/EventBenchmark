using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace Client.Workers
{

    /*
     * A default data ingestor that works for all use cases
     */
    public class DataIngestor
    {

        // variables
        // url, http client, a payload

        // actions -> http post

        // a set of urls, for each url, a respective list of objects
        public DataIngestor(IList<string> url, HttpClient httpClient, Dictionary<string,IList<object>> UrlToObjectsMap )
        {

        }

        public async Task Run()
        {

            // control the qmount of threads


        }


    }
}
