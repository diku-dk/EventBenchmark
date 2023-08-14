using Common.DataGeneration;
using Common.Workflow;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
using Common.Ingestion.Config;
using Common.Workload;
using Common.Infra;
using Common.Collection;
using Common.Cleaning;
using Common.Experiment;
//using Orleans.Workload;

namespace Common
{

    public record MasterConfig(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig, WorkloadConfig workloadConfig, CollectionConfig collectionConfig, CleaningConfig cleaningConfig);

    public class Program
    {
        private static readonly ILogger logger = LoggerProxy.GetInstance("Program");

        public static void Main_(string[] args)
        {
            DateTime startTime = DateTime.ParseExact("2023-08-04 15:57:14", "yyyy-MM-dd HH:mm:ss",
                                       System.Globalization.CultureInfo.InvariantCulture);

            var epochPeriod = 2000;
            DateTime finishTime = DateTime.ParseExact("2023-08-04 15:57:24", "yyyy-MM-dd HH:mm:ss",
                                       System.Globalization.CultureInfo.InvariantCulture);

            DateTime endTime = DateTime.ParseExact("2023-08-04 15:57:05", "yyyy-MM-dd HH:mm:ss",
                                     System.Globalization.CultureInfo.InvariantCulture);

            var span = endTime.Subtract(startTime);
            int idx = (int)(span.TotalMilliseconds / epochPeriod);

            Console.WriteLine(idx);


        }

        public static async Task Main(string[] args)
        {
            logger.LogInformation("Initializing benchmark driver...");
            var masterConfig_ = BuildMasterConfig(args);
            logger.LogInformation("Configuration parsed.");

            if(masterConfig_.Item2 is not null)
            {
                logger.LogInformation("Starting experiment...");
                //var workflowOrc = new ActorExperimentManager(masterConfig_.Item2);
                //await workflowOrc.Run();
                logger.LogInformation("Experiment finished!");
            } else
            {
                var masterConfig = masterConfig_.Item1;
                logger.LogInformation("Starting workflow...");
                // await WorkflowOrchestrator.Run(masterConfig.workflowConfig, masterConfig.syntheticDataConfig, masterConfig.ingestionConfig,
                //    masterConfig.workloadConfig, masterConfig.collectionConfig, masterConfig.cleaningConfig);
                logger.LogInformation("Workflow finished!");
            }
         
        }

        /**
         * 1 - process directory of config files in the args. if not, select the default config files
         * 2 - parse config files
         * 3 - if files parsed are correct, then start orleans
         */
        public static (MasterConfig, ExperimentConfig) BuildMasterConfig(string[] args)
        {
            string initDir = Directory.GetCurrentDirectory();
            string configFilesDir;
            if (args is not null && args.Length > 0) {
                configFilesDir = args[0];
                logger.LogInformation("Directory of configuration files passsed as parameter: {0}", configFilesDir);
            } else
            {
                configFilesDir = initDir;
            }
            Environment.CurrentDirectory = configFilesDir;

            if (File.Exists("experiment_config.json"))
            {
                /** =============== Workflow config file ================= */
                logger.LogInformation("Init reading experiment configuration file...");
                ExperimentConfig experimentConfig;
                using (StreamReader r = new StreamReader("experiment_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("experiment_config.json contents:\n {0}", json);
                    experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
                }
                logger.LogInformation("Workflow configuration file read succesfully");

                if(experimentConfig.enabled)
                    return (null, experimentConfig);
            }

            if (!File.Exists("workflow_config.json"))
            {
                throw new Exception("Workflow configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("data_load_config.json"))
            {
                throw new Exception("Data load configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("ingestion_config.json"))
            {
                throw new Exception("Ingestion configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("workload_config.json"))
            {
                throw new Exception("Workload configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("collection_config.json"))
            {
                throw new Exception("Collection of metrics configuration file cannot be loaded from " + configFilesDir);
            }
            if (!File.Exists("cleaning_config.json"))
            {
                throw new Exception("Cleaning configuration file cannot be loaded from " + configFilesDir);
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
                foreach (var entry in list)
                {
                    if (entry.Value < lastPerc) throw new Exception("Transaction distribution is incorrectly configured.");
                    lastPerc = entry.Value;
                }
                if (list.Last().Value != 100) throw new Exception("Transaction distribution is incorrectly configured.");

                /**
                 * The relation between prescribed concurrency level and minimum number of customers
                 * varies according to the distribution of transactions. The code below is a (very)
                 * safe threshold.
                 */
                if (dataLoadConfig != null && dataLoadConfig.numCustomers < workloadConfig.concurrencyLevel)
                {
                    throw new Exception("Total number of customers must be higher than concurrency level");
                }

                if (workloadConfig.customerWorkerConfig.productUrl is null)
                {
                    throw new Exception("No products URL found! Execution suspended.");
                }
                if (workloadConfig.customerWorkerConfig.cartUrl is null)
                {
                    throw new Exception("No carts URL found! Execution suspended.");
                }
                if (workloadConfig.sellerWorkerConfig.productUrl is null)
                {
                    throw new Exception("No product URL found for seller! Execution suspended.");
                }
                if (workloadConfig.sellerWorkerConfig.sellerUrl is null)
                {
                    throw new Exception("No sellers URL found! Execution suspended.");
                }
                if (workloadConfig.deliveryWorkerConfig.shipmentUrl is null)
                {
                    throw new Exception("No shipment URL found! Execution suspended.");
                }

            }

            /** =============== Collection of metrics config file ================= */
            CollectionConfig collectionConfig = null;
            if (workflowConfig.collection)
            {
                logger.LogInformation("Init reading collection of metrics configuration file...");
                using (StreamReader r = new StreamReader("collection_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("collection_config.json contents:\n {0}", json);
                    collectionConfig = JsonConvert.DeserializeObject<CollectionConfig>(json);
                }
                logger.LogInformation("Collection of metrics file read succesfully");
            }


            /** =============== Cleaning config file ================= */
            CleaningConfig cleaningConfig = null;
            if (workflowConfig.cleanup)
            {
                logger.LogInformation("Init reading cleaning configuration file...");
                using (StreamReader r = new StreamReader("cleaning_config.json"))
                {
                    string json = r.ReadToEnd();
                    logger.LogInformation("cleaning_config.json contents:\n {0}", json);
                    cleaningConfig = JsonConvert.DeserializeObject<CleaningConfig>(json);
                }
                logger.LogInformation("Cleaning file read succesfully");
            }

            MasterConfig masterConfiguration = new MasterConfig(            
                workflowConfig,dataLoadConfig, ingestionConfig, workloadConfig, collectionConfig, cleaningConfig);
            return (masterConfiguration, null);
        }

    }

}