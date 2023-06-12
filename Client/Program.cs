using Client.DataGeneration;
using Client.Execution;
using Client.Infra;
using Client.Server;
using Common.Http;
using Common.Ingestion;
using Common.Ingestion.Config;
using Common.Scenario;
using Common.Entities;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Linq;

namespace Client
{

    /**
     * Main method based on 
     * http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
     */
    public class Program
    {
        private static ILogger logger = LoggerFactory.Create(
                        b => b
                            .AddConsole()
                            .AddFilter(level => level >= LogLevel.Information))
                            .CreateLogger("Main");

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
            ["sellers"] = "http://127.0.0.1:8001/sellers",
            ["customers"] = "http://127.0.0.1:8001/customers",
            ["shipments"] = "http://127.0.0.1:8001/shipments",
        };

        private static readonly ScenarioConfiguration defaultScenarioConfig = new()
        {
            transactionDistribution = new Dictionary<TransactionType, int>()
            {
                [TransactionType.CUSTOMER_SESSION] = 70,
                [TransactionType.UPDATE_DELIVERY] = 100
            },
            mapTableToUrl = mapTableToUrl,
            customerWorkerConfig = new()
            {
                maxNumberKeysToBrowse = 10,
                maxNumberKeysToAddToCart = 10, // both the same for simplicity
                sellerDistribution = Common.Configuration.Distribution.UNIFORM,
                // set dynamically by master orchestrator
                // sellerRange = new Range(1, 10),
                urls = mapTableToUrl,
                minMaxQtyRange = new Interval(1, 10),
                delayBetweenRequestsRange = new Interval(1, 1000),
                delayBeforeStart = 0,
                checkoutProbability = 50
            },
            sellerWorkerConfig = new() {
                keyDistribution = Common.Configuration.Distribution.UNIFORM,
                urls = mapTableToUrl,
                delayBetweenRequestsRange = new Interval(1, 1000)
            },
            submissionType = SubmissionEnum.QUANTITY,
            submissionValue = 5,// 1,
            executionTime = 60000, // 1 min
            waitBetweenSubmissions = 10000, // 60000, // 60 seconds
            customerDistribution = Common.Configuration.Distribution.UNIFORM,
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

            logger.LogInformation("start testing conversion type");

            using (var duckDBConnection = new DuckDBConnection("DataSource=file.db"))
            {
                List<Product> products = DuckDbUtils.SelectAll<Product>(duckDBConnection, "products");
                foreach(var product in products)
                    logger.LogInformation(JsonConvert.SerializeObject(product));
            }
        }

        /**
         * 1 - process directory of config files in the args. if not, select the default config files
         * 2 - parse config files
         * 3 - if files parsed are correct, then start orleans
         */
        public static MasterConfiguration BuildMasterConfig(string[] args)
        {
            
            logger.LogInformation("Initializing benchmark driver...");

            string initDir = Directory.GetCurrentDirectory();
            string configFilesDir;
            if (args is not null && args.Length > 0){
                configFilesDir = args[0];
                logger.LogInformation("Directory of configuration files passsed as parameter: {0}", configFilesDir);
                Environment.CurrentDirectory = (configFilesDir);
            } else
            {
                configFilesDir = Directory.GetCurrentDirectory();
            }

            if (!File.Exists("workflow_config.json"))
            {
                throw new Exception("Workflow configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("data_load_config.json"))
            {
                throw new Exception("Data load configuration file cannot be loaded from "+ configFilesDir);
            }
            if (!File.Exists("ingestion_config.json"))
            {
                throw new Exception("Ingestion configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("scenario_config.json"))
            {
                throw new Exception("Scenario configuration file cannot be loaded from " + configFilesDir);
            }

            /** =============== Workflow config file ================= */
            logger.LogInformation("Init reading workflow configuration file...");
            WorkflowConfig workflowConfig;
            using (StreamReader r = new StreamReader("workflow_config.json"))
            {
                string json = r.ReadToEnd();
                logger.LogInformation("workflow_config.json contents:\n {0}", json);
                workflowConfig = JsonConvert.DeserializeObject<WorkflowConfig>(json);
            }
            logger.LogInformation("Workflow configuration file read succesfully");

            /** =============== Data load config file ================= */
            
            SyntheticDataSourceConfiguration dataLoadConfig = null;
            if (workflowConfig.dataLoad)
            {
                logger.LogInformation("Init reading data load configuration file...");
                using (StreamReader r = new StreamReader("data_load_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("data_load_config.json contents:\n {0}", json);
                    dataLoadConfig = JsonConvert.DeserializeObject<SyntheticDataSourceConfiguration>(json);
                }
                logger.LogInformation("Data load configuration file read succesfully");
            }

            /** =============== Ingestion config file ================= */
            IngestionConfiguration ingestionConfig = null;
            if (workflowConfig.ingestion)
            {
                logger.LogInformation("Init reading ingestion configuration file...");
                using (StreamReader r = new StreamReader("ingestion_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("ingestion_config.json contents:\n {0}", json);
                    ingestionConfig = JsonConvert.DeserializeObject<IngestionConfiguration>(json);
                }
                logger.LogInformation("Ingestion configuration file read succesfully");
            }

            /** =============== Scenario config file ================= */
            ScenarioConfiguration scenarioConfig = null;
            if (workflowConfig.transactionSubmission)
            {
                logger.LogInformation("Init reading scenario configuration file...");
                using (StreamReader r = new StreamReader("scenario_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("scenario_config.json contents:\n {0}", json);
                    scenarioConfig = JsonConvert.DeserializeObject<ScenarioConfiguration>(json);
                }
                logger.LogInformation("Scenario file read succesfully");

                int total = scenarioConfig.transactionDistribution.Values.Sum();
                if(total != 100)
                {
                    throw new Exception("Total distribution must sum to 100, not " + total);
                }

            }

            MasterConfiguration masterConfiguration = new()
            {
                workflowConfig = workflowConfig,
                scenarioConfig = scenarioConfig,
                ingestionConfig = ingestionConfig,
                syntheticDataConfig = dataLoadConfig
                // olistConfig = new DataGeneration.Real.OlistDataSourceConfiguration()
            };
            return masterConfiguration;
        }

        private static readonly bool mock = false;

        /*
         * 
         */
        public static async Task Main(string[] args)
        {

            var masterConfiguration = BuildMasterConfig(args);
            /*
             1st phase

             Let all services get db connection from app settings (adapt all dbcontext) OK
             Adapt driver/workers to inputs OK

             Review workers.. their input/output/reactions

             Spawn driver to generate requests to dapr
             - let dapr services receiving transactions [can be few to start and then keep adding]
                — cart, stock, order
             — leave customer reaction for later
             - check for errors

             2nd phase

             Prepare environments
             Deploy driver in VM
             Start postgres ku cloud
             Deploy all dapr services
            */

            logger.LogInformation("Initializing Orleans client...");
            var client = await OrleansClientFactory.Connect();
            if (client == null) return;
            logger.LogInformation("Orleans client initialized!");

            HttpServer httpServer = null;
            if (mock) {
                logger.LogInformation("Initializing Mock Http server...");
                httpServer = new HttpServer(new DebugHttpHandler());
                Task httpServerTask = Task.Run(httpServer.Run);
            }

            MasterOrchestrator orchestrator = new MasterOrchestrator(client, masterConfiguration, logger);
            await orchestrator.Run();

            logger.LogInformation("Master orchestrator finished!");

            await client.Close();

            logger.LogInformation("Orleans client finalized!");

            if (mock)
            {
                httpServer.Stop();
            }
        }

    }

}