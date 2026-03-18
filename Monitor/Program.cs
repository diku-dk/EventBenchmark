using System;
using System.Linq;
using System.Threading.Tasks;
using Common.Experiment;
using Common.Http;
using Common.Infra;
using DuckDB.NET.Data;

namespace Monitor;

public class Program
{

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Initializing benchmark driver...");
        ExperimentConfig config = ConsoleUtility.BuildExperimentConfig(args);
        Console.WriteLine("Configuration parsed. Starting program...");
        DuckDBConnection connection = null;

        try{
        while(true){

        Console.WriteLine("\n Select an option: \n 1 - Generate Data \n 2 - Run Experiment \n 5 - Parse New Configuration \n q - Exit");
        string op = Console.ReadLine();

        switch (op)
        {
            case "1":
            {
                connection = ConsoleUtility.GenerateData(config);
                break;
            }
            case "2":
            {
                if(connection is null) {
                    if(config.connectionString.SequenceEqual("DataSource=:memory:"))
                    {
                        Console.WriteLine("Please generate some data first by selecting option 1.");
                        break;
                    }
                    else
                    {
                        connection = new DuckDBConnection(config.connectionString);
                        connection.Open();
                    }
                }
        
                var expManager = MonitorExperimentManager.BuildExperimentManager(config, connection);
                if(config.delayBetweenRuns > 0)
                {
                    Console.WriteLine($"Delay of {config.delayBetweenRuns} ms after ingest.");
                    await Task.Delay(config.delayBetweenRuns);
                }
                // run
                expManager.RunMonitorExperiment();
                Console.WriteLine("Experiment finished.");
                break;
            }
            case "5":
            {
                config = ConsoleUtility.BuildExperimentConfig(args);
                Console.WriteLine("Configuration parsed.");
                break;
            }
            case "q":
            {
                return;
            }
            default:
            {
                Console.WriteLine("Input invalid");
                break;
            }
        }
        }
        } catch(Exception e)
        {
            Console.WriteLine("Exception catched. Source: {0}; StackTrace: \n {1}", e.Source, e.StackTrace );
        }
    }

}

