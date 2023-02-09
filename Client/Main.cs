using Common.Ingestion;
using GrainInterfaces.Ingestion;
using Orleans;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Client
{

    /**
     * Based on http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
     */
    public class Program
    {

        private static bool running;

        private static readonly IngestionConfiguration defaultIngestionConfig = new()
        {
            dataNatureType = DataSourceType.SYNTHETIC,
            partitioningStrategy = IngestionPartitioningStrategy.SINGLE_WORKER,
            numberCpus = 2,
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["warehouse"] = "http://127.0.0.1:8001/data",
                ["districts"] = "http://127.0.0.1:8001/data",
                ["items"] = "http://127.0.0.1:8001/data"

                /*
                ["customers"] = "http://127.0.0.1:8001/data",
                ["stockItems"] = "http://127.0.0.1:8001/data", */
            }
        };

        static void Main(string[] args)
        {
            Task.Run(() => InitializeOrleans());

            /*
            HttpClient client = new HttpClient();
            try
            {
                using HttpResponseMessage response = client.PostAsync("http://127.0.0.1:8001/data", new StringContent("TESTE", Encoding.UTF8, "application/json")).Result;
                response.EnsureSuccessStatusCode();
                Console.WriteLine("Here we are: " + response.StatusCode);
                
            }
            catch (HttpRequestException e)
            {
                Console.WriteLine("\nException Caught!");
                Console.WriteLine("Message :{0} ", e.Message);
                Console.WriteLine(e.StatusCode.Value);
            }
            */

            Console.ReadLine();

            running = false;
        }

        static async Task InitializeOrleans()
        {

            var client = await ConnectClient();
            
            var ingestionOrchestrator = client.GetGrain<IIngestionOrchestrator>(1);

            Console.WriteLine("Ingestion orchestrator grain obtained.");

            await ingestionOrchestrator.Run(defaultIngestionConfig);

            Console.WriteLine("Ingestion orchestrator grain finished.");

            // TODO setup grains with default or provided config

            // TODO bulk data ingestor grain... maybe not necessary now, just a thread pool with a thread per microservice....

            // setup rabbitmq client after generating the data

            await client.Close();
        }

        public static async Task<IClusterClient> ConnectClient()
        {
            Console.WriteLine("Initializing...");

            var client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .Build();
            await client.Connect();

            running = true;
            Console.WriteLine("Initialized!");

            return client;
        }

    }

}
