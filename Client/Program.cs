using Client.DataGeneration;
using Client.Workflow;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;
using System.IO;
using Microsoft.Extensions.Logging;
using System.Linq;
using Client.Ingestion.Config;
using Client.Workload;
using Common.Infra;
using Client.Collection;
using Client.Cleaning;
using Client.Experiment;

namespace Client
{

    public record MasterConfig(WorkflowConfig workflowConfig, SyntheticDataSourceConfig syntheticDataConfig, IngestionConfig ingestionConfig,
        WorkloadConfig workloadConfig, CollectionConfig collectionConfig, CleaningConfig cleaningConfig);

    public class Program
    {
        private static readonly ILogger logger = LoggerProxy.GetInstance("Program");

        public static async Task Main_(string[] args)
        {
            var masterConfig = BuildMasterConfig(args);

            // http://localhost:9090/api/v1/query?query=dapr_component_pubsub_ingress_count&time=1688568061.327

            var res = await MetricGather.GetFromPrometheus(masterConfig.Item1.collectionConfig, "1688568061.327");
            /*
            var message = new HttpRequestMessage(HttpMethod.Get, "http://localhost:9090/api/v1/query?query=dapr_component_pubsub_egress_count{app_id=%22cart%22,topic=%22ReserveStock%22}&time=1688136844");
            // masterConfig.collectionConfig.baseUrl + "/" + masterConfig.collectionConfig.ingress_count);
            var resp = HttpUtils.client.Send(message);
            string respStr = await resp.Content.ReadAsStringAsync();
            dynamic respObj = JsonConvert.DeserializeObject<ExpandoObject>(respStr);

            ExpandoObject data = respObj.data.result[0];
            var elem = ((List<object>)data.ElementAt(1).Value)[1];

            Console.WriteLine("the respObj is {0}", respObj);
            */

            Console.WriteLine("the respObj is {0}", res);
        }

        /*
         * 
         */
        public static async Task Main(string[] args)
        {
            logger.LogInformation("Initializing benchmark driver...");
            var masterConfig_ = BuildMasterConfig(args);
            logger.LogInformation("Configuration parsed.");

            if(masterConfig_.Item2 is not null)
            {
                logger.LogInformation("Starting experiment...");
                await WorkflowOrchestrator.Run(masterConfig_.Item2);
                logger.LogInformation("Experiment finished!");
            } else
            {
                var masterConfig = masterConfig_.Item1;
                logger.LogInformation("Starting workflow...");
                await WorkflowOrchestrator.Run(masterConfig.workflowConfig, masterConfig.syntheticDataConfig, masterConfig.ingestionConfig,
                    masterConfig.workloadConfig, masterConfig.collectionConfig, masterConfig.cleaningConfig);
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