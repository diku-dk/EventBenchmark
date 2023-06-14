using Client.DataGeneration;
using Client.Workflow;
using Client.Infra;
using Common.Http;
using Common.Ingestion;
using Common.Workload;
using Common.Entities;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Linq;
using Client.Http;
using Client.Ingestion.Config;
using Client.Workload;
using StackExchange.Redis;
using Microsoft.Extensions.Hosting;

namespace Client
{

    /**
     * Main method based on 
     * http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
     */
    public class Program
    {
        private static ILogger logger = LoggerProxy.GetInstance("Program");

        public static void Main_(string[] args)
        {
            ConnectionMultiplexer redis = ConnectionMultiplexer.Connect("localhost");

        }

        public record MasterConfig(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig, WorkloadConfig workloadConfig);

        /**
         * 1 - process directory of config files in the args. if not, select the default config files
         * 2 - parse config files
         * 3 - if files parsed are correct, then start orleans
         */
        public static MasterConfig BuildMasterConfig(string[] args)
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
            if (!File.Exists("workload_config.json"))
            {
                throw new Exception("Workload configuration file cannot be loaded from " + configFilesDir);
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
            
            SyntheticDataSourceConfig dataLoadConfig = null;
            if (workflowConfig.dataLoad)
            {
                logger.LogInformation("Init reading data load configuration file...");
                using (StreamReader r = new StreamReader("data_load_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("data_load_config.json contents:\n {0}", json);
                    dataLoadConfig = JsonConvert.DeserializeObject<SyntheticDataSourceConfig>(json);
                }
                logger.LogInformation("Data load configuration file read succesfully");
            }

            /** =============== Ingestion config file ================= */
            IngestionConfig ingestionConfig = null;
            if (workflowConfig.ingestion)
            {
                logger.LogInformation("Init reading ingestion configuration file...");
                using (StreamReader r = new StreamReader("ingestion_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("ingestion_config.json contents:\n {0}", json);
                    ingestionConfig = JsonConvert.DeserializeObject<IngestionConfig>(json);
                }
                logger.LogInformation("Ingestion configuration file read succesfully");
            }

            /** =============== Workload config file ================= */
            WorkloadConfig workloadConfig = null;
            if (workflowConfig.transactionSubmission)
            {
                logger.LogInformation("Init reading scenario configuration file...");
                using (StreamReader r = new StreamReader("workload_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("scenario_config.json contents:\n {0}", json);
                    workloadConfig = JsonConvert.DeserializeObject<WorkloadConfig>(json);
                }
                logger.LogInformation("Scenario file read succesfully");

                var list = workloadConfig.transactionDistribution.ToList();
                int lastPerc = 0;
                foreach(var entry in list)
                {
                    if (entry.Value < lastPerc) throw new Exception("Transaction distribution is incorrectly configured.");
                    lastPerc = entry.Value;
                }
                if(list.Last().Value != 100) throw new Exception("Transaction distribution is incorrectly configured.");

                /**
                 * The relation between prescribed concurrency level and minimum number of customers
                 * varies according to the distribution of transactions. The code below is a (very)
                 * safe threshold.
                 */
                if (dataLoadConfig.numCustomers < workloadConfig.concurrencyLevel)
                {
                    throw new Exception("Total number of customers must be higher than concurrency level");
                }

            }

            MasterConfig masterConfiguration = new MasterConfig(            
                workflowConfig,dataLoadConfig, ingestionConfig, workloadConfig
                // olistConfig = new DataGeneration.Real.OlistDataSourceConfiguration()
            );
            return masterConfiguration;
        }

        private static readonly bool mock = false;

        /*
         * 
         */
        public static async Task Main(string[] args)
        {

            var masterConfig = BuildMasterConfig(args);

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

            WorkflowOrchestrator orchestrator = new WorkflowOrchestrator(client, masterConfig.workflowConfig, masterConfig.syntheticDataConfig, masterConfig.ingestionConfig, masterConfig.workloadConfig);
            await orchestrator.Run();

            logger.LogInformation("Workflow orchestrator finished!");

            await client.Close();

            logger.LogInformation("Orleans client finalized!");

            if (mock)
            {
                httpServer.Stop();
            }
        }

    }

}