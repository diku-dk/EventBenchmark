using Client.DataGeneration;
using Client.Execution;
using Client.Infra;
using Client.Server;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Streaming;
using DuckDB.NET.Data;
using Orleans;
using Orleans.Hosting;
using Orleans.Runtime.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Client
{

    public class Program
    {

        private static readonly IngestionConfiguration defaultIngestionConfig = new()
        {
            distributionStrategy = IngestionDistributionStrategy.SINGLE_WORKER,
            numberCpus = 2,
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["categories"] = "http://127.0.0.1:8001/categories",
                ["sellers"] = "http://127.0.0.1:8001/sellers",
                ["products"] = "http://127.0.0.1:8001/products",
                ["customers"] = "http://127.0.0.1:8001/customers",
                ["stock_items"] = "http://127.0.0.1:8001/stock_items",
            }
        };

        private static Dictionary<string, string> mapTableToUrl = new Dictionary<string, string>()
        {
            ["products"] = "http://127.0.0.1:8001/products",
            ["carts"] = "http://127.0.0.1:8001/carts",
            ["sellers"] = "http://127.0.0.1:8001/sellers"
        };

        private static readonly ScenarioConfiguration defaultScenarioConfig = new()
        {
            weight = new WorkloadType[] { WorkloadType.CUSTOMER_SESSION },
            mapTableToUrl = mapTableToUrl,
            customerConfig = new()
            {
                maxNumberKeysToBrowse = 10,
                maxNumberKeysToAddToCart = 10, // both the same for simplicity
                sellerDistribution = Common.Configuration.Distribution.UNIFORM,
                // sellerRange = new Range(1, 15), // this is defined dynamically
                urls = mapTableToUrl,
                minMaxQtyRange = new Range(1, 11),
                delayBetweenRequestsRange = new Range(1, 1000),
                delayBeforeStart = 0
            },
            sellerConfig = new() {
                keyDistribution = Common.Configuration.Distribution.UNIFORM,
                urls = mapTableToUrl,
                delayBetweenRequestsRange = new Range(1, 1000),
            },
            submissionType = SubmissionEnum.QUANTITY,
            windowOrBurstValue = 1,
            period = TimeSpan.FromSeconds(600), // 10 min
            waitBetweenSubmissions = 60000 // 60 seconds
        };

        public static void Main_(string[] args)
        {

            SyntheticDataGenerator syntheticDataGenerator = new SyntheticDataGenerator(new SyntheticDataSourceConfiguration());
            syntheticDataGenerator.Generate();

            using (var duckDBConnection = new DuckDBConnection("DataSource=file.db"))
            {
                duckDBConnection.Open();
                var command = duckDBConnection.CreateCommand();
                command.CommandText = "select rowid, * from customers LIMIT 10;";
                var executeNonQuery = command.ExecuteNonQuery();
                var reader = command.ExecuteReader();
                DuckDbUtils.PrintQueryResults(reader);
            }
            
        }


        /**
         * Main method based on 
         * http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
         */
        public static async Task Main(string[] args)
        {
            Console.WriteLine("Initializing Orleans client...");
            var client = await OrleansClientFactory.Connect();
            if (client == null) return;
            Console.WriteLine("Orleans client initialized!");

            Console.WriteLine("Initializing Mock Http server...");
            HttpServer httpServer = new HttpServer();
            Task httpServerTask = Task.Run(() => { httpServer.Run(); });

            SyntheticDataSourceConfiguration syntheticConfig = new SyntheticDataSourceConfiguration();
            // syntheticConfig.fileDir = Environment.GetEnvironmentVariable("HOME") + "/workspace/EventBenchmark/Client/DataGeneration/Synthetic";

            MasterConfiguration masterConfiguration = new()
            {
                orleansClient = client,
                streamEnabled = false,
                load = true,
                healthCheck = false,
                ingestion = true,
                transaction = false,
                cleanup = false,
                scenarioConfig = defaultScenarioConfig,
                ingestionConfig = defaultIngestionConfig,
                // syntheticConfig = syntheticConfig
                olistConfig = new DataGeneration.Real.OlistDataSourceConfiguration()
            };

            MasterOrchestrator orchestrator = new MasterOrchestrator(masterConfiguration);
            await orchestrator.Run();

            Console.WriteLine("Master orchestrator finished!");

            await client.Close();

            httpServer.Stop();

        }

    }

}