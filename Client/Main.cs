using Client.Server;
using Common.Entities.TPC_C;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Ingestion.DataGeneration;
using Common.Ingestion.DTO;
using Common.Scenario;
using Common.Serdes;
using Common.Streaming;
using GrainInterfaces.Ingestion;
using Newtonsoft.Json;
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
            dataSourceType = DataSourceType.SYNTHETIC,
            distributionStrategy = IngestionDistributionStrategy.SINGLE_WORKER,
            numberCpus = 2,
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["warehouses"] = "http://127.0.0.1:8001/warehouses",
                ["districts"] = "http://127.0.0.1:8001/districts",
                ["items"] = "http://127.0.0.1:8001/items",
                // ["healthCheck"] = "http://127.0.0.1:8001/healthCheck"
                /*
                ["customers"] = "http://127.0.0.1:8001/data",
                ["stockItems"] = "http://127.0.0.1:8001/data",
                */
            }
        };

        private static readonly ScenarioConfiguration defaultScenarioConfig = new()
        {
            weight = new TransactionType[] { TransactionType.CHECKOUT },
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["product"] = "http://127.0.0.1:8001/product",
                ["cart"] = "http://127.0.0.1:8001/cart",
            }
        };

        public static async Task Main_(string[] args)
        {
             GeneratedData data = SyntheticDataGenerator.Generate(SerdesFactory.build());

            // var ware = data.tables["warehouses"];
            var item = new Item(1, 1, "frfrf", 34f, "frfrfr");
            var str = JsonConvert.SerializeObject(item);

            Console.WriteLine(str);

            var deser = JsonConvert.DeserializeObject(str);

            Console.WriteLine(deser.GetType().ToString());

        }

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
                streamEnabled = false,
                healthCheck = false,
                ingestion = true,
                transactionSubmission = false
            };

            MasterOrchestrator orchestrator = new MasterOrchestrator(masterConfiguration, defaultIngestionConfig, defaultScenarioConfig);
            Task masterTask = Task.Run(async () => { await orchestrator.Run(); });
            await masterTask;

            Console.WriteLine("Master orchestrator finished!");

            await client.Close();

            httpServer.Stop();
            // await httpServerTask;

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
            IClusterClient client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .AddSimpleMessageStreamProvider(StreamingConfiguration.DefaultStreamProvider, options =>
                                {
                                    options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit;
                                    options.FireAndForgetDelivery = false;
                                    //options.OptimizeForImmutableData = true;
                                })
                                .Build();

            Func<Exception, Task<bool>> func = (x) => {
                return Task.FromResult(false);
            };

            Task connectTask = client.Connect(func);
            await connectTask;
            return client;
        }

    }

}