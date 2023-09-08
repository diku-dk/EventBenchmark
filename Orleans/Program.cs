using Common.Experiment;
using Common.Infra;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Orleans.Infra;
using Orleans.Workload;

namespace Orleans;

public class Program
{
    private static readonly ILogger logger = LoggerProxy.GetInstance("Program");

    public static async Task Main(string[] args)
    {
        logger.LogInformation("Initializing benchmark driver...");
        var config = BuildExperimentConfig(args);
        logger.LogInformation("Configuration parsed.");
        logger.LogInformation("Starting experiment...");
        var workflowOrc = new ActorExperimentManager(new CustomHttpClientFactory(), config);
        await workflowOrc.Run();
        logger.LogInformation("Experiment finished!");
    }

    public static ExperimentConfig BuildExperimentConfig(string[] args)
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
            logger.LogInformation("Init reading experiment configuration file...");
            ExperimentConfig experimentConfig;
            using (StreamReader r = new StreamReader("experiment_config.json"))
            {
                string json = r.ReadToEnd();
                logger.LogInformation("experiment_config.json contents:\n {0}", json);
                experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
            }
            logger.LogInformation("Workflow configuration file read succesfully");

            return experimentConfig;
        }
        throw new Exception("Experiment config file not found");
    }

}


