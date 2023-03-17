using Client.Server;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Streaming;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime.Messaging;
using System;
using System.Collections.Generic;
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
                ["products"] = "http://127.0.0.1:8001/products",
                ["carts"] = "http://127.0.0.1:8001/carts",
            },
            submissionType = SubmissionEnum.QUANTITY,
            windowOrBurstValue = 1,
            period = TimeSpan.FromSeconds(600), // 10 min
            waitBetweenSubmissions = 60000 // 60 seconds
    };

        public static async Task Main(string[] args)
        {
            Console.WriteLine("Initializing Orleans client...");
            var client = await ConnectClient();
            if (client == null) return;
            Console.WriteLine("Orleans client initialized!");

            Console.WriteLine("Initializing Mock Http server...");
            HttpServer httpServer = new HttpServer();
            Task httpServerTask = Task.Run(() => { httpServer.Run(); });

            MasterConfiguration masterConfiguration = new()
            {
                orleansClient = client,
                streamEnabled = false,
                healthCheck = false,
                ingestion = true,
                transactionSubmission = true,
                cleanup = false
            };

            MasterOrchestrator orchestrator = new MasterOrchestrator(masterConfiguration, defaultIngestionConfig, defaultScenarioConfig);
            await orchestrator.Run();

            Console.WriteLine("Master orchestrator finished!");

            await client.Close();

            httpServer.Stop();

        }

        public static async Task<IClusterClient> ConnectClient()
        {
            IClusterClient client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .AddSimpleMessageStreamProvider(StreamingConfiguration.DefaultStreamProvider, options =>
                                {
                                    options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedOnly;
                                    options.FireAndForgetDelivery = false;
                                    //options.OptimizeForImmutableData = true;
                                })
                                .Build();

            Func<Exception, Task<bool>> func = (x) => {
                return Task.FromResult(false);
            };

            try
            {
                Task connectTask = client.Connect(func);
                await connectTask;
                return client;
            } catch(ConnectionFailedException e)
            {
                Console.WriteLine("Error connecting to Silo: {0}", e.Message);
            }
            return null;
        }

    }

}