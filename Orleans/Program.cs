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
        logger.LogInformation("Configuration parsed. Starting experiment...");
        var expManager = new ActorExperimentManager(new CustomHttpClientFactory(), config);
        await expManager.Run();
        logger.LogInformation("Experiment finished.");
    }

    public static ExperimentConfig BuildExperimentConfig(string[] args)
    {
        if (args is not null && args.Length > 0 && File.Exists(args[0])) {
            logger.LogInformation("Directory of configuration files passsed as parameter: {0}", args[0]);
        } else
        {
            throw new Exception("No file passed as parameter!");
        }

        logger.LogInformation("Init reading experiment configuration file...");
        ExperimentConfig experimentConfig;
        using (StreamReader r = new StreamReader(args[0]))
        {
            string json = r.ReadToEnd();
            logger.LogInformation("Configuration file contents:\n {0}", json);
            experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
        }
        logger.LogInformation("Experiment configuration read succesfully");

        return experimentConfig;
        
    }

}


