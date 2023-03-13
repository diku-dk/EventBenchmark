using Client.Server;
using Common.Ingestion;
using Common.Ingestion.Worker;
using Common.Scenario;
using Common.Streaming;
using GrainInterfaces.Ingestion;
using Orleans;
using Orleans.Hosting;
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

        private static readonly IngestionConfiguration defaultIngestionConfig = new()
        {
            dataNatureType = DataSourceType.SYNTHETIC,
            partitioningStrategy = IngestionPartitioningStrategy.SINGLE_WORKER,
            numberCpus = 2,
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["warehouse"] = "http://127.0.0.1:8001/data",
                ["districts"] = "http://127.0.0.1:8001/data",
                ["items"] = "http://127.0.0.1:8001/data",
                ["healthCheck"] = "http://127.0.0.1:8001/healthCheck"
                /*
                ["customers"] = "http://127.0.0.1:8001/data",
                ["stockItems"] = "http://127.0.0.1:8001/data",
                */
            }
        };

        private static readonly ScenarioConfiguration defaultScenarioConfig = new()
        {
            weight = new TransactionType[] { TransactionType.CHECKOUT },
            mapTableToUrl = defaultIngestionConfig.mapTableToUrl
        };

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Initializing Mock Http server...");
            HttpServer httpServer = new HttpServer();
            Task httpServerTask = Task.Run(() => { httpServer.Run(); });

            Console.WriteLine("Initializing Orleans client...");
            var client = await ConnectClient();
            Console.WriteLine("Orleans client initialized!");

            MasterConfiguration masterConfiguration = new()
            {
                orleansClient = client,
                streamEnabled = false
            };

            MasterOrchestrator orchestrator = new MasterOrchestrator(masterConfiguration, defaultIngestionConfig, defaultScenarioConfig);
            await orchestrator.Run();

            await client.Close();

            httpServer.Stop();
            await httpServerTask;

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


        }

        public static async Task<IClusterClient> ConnectClient()
        {
            var client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .AddSimpleMessageStreamProvider(StreamingConfiguration.defaultStreamProvider, options =>
                                {
                                    options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit;
                                    options.FireAndForgetDelivery = false;
                                })
                                .Build();
            await client.Connect();
            return client;
        }

    }

}
