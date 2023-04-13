using Client.DataGeneration;
using Client.Execution;
using Client.Infra;
using Client.Server;
using Common.Http;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Scenario.Entity;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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
                ["sellers"] = "http://127.0.0.1:8001/sellers",
                ["customers"] = "http://127.0.0.1:8001/customers",
                ["stock_items"] = "http://127.0.0.1:8001/stock_items",
                ["products"] = "http://127.0.0.1:8001/products",
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
                sellerRange = new Range(1, 10),
                urls = mapTableToUrl,
                minMaxQtyRange = new Range(1, 11),
                delayBetweenRequestsRange = new Range(1, 1000),
                delayBeforeStart = 0
            },
            sellerConfig = new() {
                keyDistribution = Common.Configuration.Distribution.UNIFORM,
                urls = mapTableToUrl,
                delayBetweenRequestsRange = new Range(1, 1000)
            },
            submissionType = SubmissionEnum.QUANTITY,
            submissionValue = 1,
            period = TimeSpan.FromSeconds(600), // 10 min
            waitBetweenSubmissions = 60000, // 60 seconds
            // TODO setup this dynamically
            customerDistribution = Common.Configuration.Distribution.UNIFORM,
            customerRange = new Range(1, 10),
        };

        public static void Main_(string[] args)
        {

            SyntheticDataGenerator syntheticDataGenerator = new SyntheticDataGenerator(new SyntheticDataSourceConfiguration());
            syntheticDataGenerator.Generate();

            /*
            using (var duckDBConnection = new DuckDBConnection("DataSource=file.db"))
            {
                duckDBConnection.Open();
                var command = duckDBConnection.CreateCommand();
                command.CommandText = "select rowid, * from customers LIMIT 10;";
                var executeNonQuery = command.ExecuteNonQuery();
                var reader = command.ExecuteReader();
                DuckDbUtils.PrintQueryResults(reader);
            }
            */

            Console.WriteLine("start testing conversion type");

            using (var duckDBConnection = new DuckDBConnection("DataSource=file.db"))
            {
                List<Product> products = DuckDbUtils.SelectAll<Product>(duckDBConnection, "products");
                foreach(var product in products)
                    Console.WriteLine(JsonConvert.SerializeObject(product));
            }
        }

        private static readonly bool mock = false;

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

            HttpServer httpServer = null;
            if (mock) {
                Console.WriteLine("Initializing Mock Http server...");
                httpServer = new HttpServer(new HttpHandler());
                Task httpServerTask = Task.Run(httpServer.Run);
            }

            MasterConfiguration masterConfiguration = new()
            {
                orleansClient = client,
                streamEnabled = false,
                load = true,
                healthCheck = false,
                ingestion = true,
                transaction = true,
                cleanup = false,
                scenarioConfig = defaultScenarioConfig,
                ingestionConfig = defaultIngestionConfig,
                syntheticConfig = new SyntheticDataSourceConfiguration()
                // olistConfig = new DataGeneration.Real.OlistDataSourceConfiguration()
        };

            MasterOrchestrator orchestrator = new MasterOrchestrator(masterConfiguration);
            await orchestrator.Run();

            Console.WriteLine("Master orchestrator finished!");

            await client.Close();

            Console.WriteLine("Orleans client finalized!");

            if (mock)
            {
                httpServer.Stop();
            }
        }

    }

}