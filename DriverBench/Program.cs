using Common.Experiment;
using Common.Http;
using Common.Infra;
using DriverBench.Experiment;
using DuckDB.NET.Data;

namespace DriverBench;

public sealed class Program
{

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Initializing benchmark driver...");
        ExperimentConfig config = ConsoleUtility.BuildExperimentConfig(args);
        Console.WriteLine("Configuration parsed. Starting program...");

        try
        {
            while (true)
            {

                Console.WriteLine("\n Select an option: \n 1 - Run Scalability Experiment \n q - Exit");
                string op = Console.ReadLine();

                switch (op)
                {
                    case "1":
                    {
                        var expManager = new DriverBenchExperimentManager(config, null);
                        await expManager.RunSimpleExperiment();
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
        }
        catch (Exception e)
        {
            Console.WriteLine("Exception catched. Source: {0}; Message: {0}", e.Source, e.StackTrace);
        }
    }

}

