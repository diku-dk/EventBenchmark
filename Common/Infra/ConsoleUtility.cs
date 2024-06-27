using Common.DataGeneration;
using Common.Experiment;
using DuckDB.NET.Data;
using Newtonsoft.Json;

namespace Common.Infra;

// 
/**
 * Bar progress source code: https://stackoverflow.com/a/70097843/7735153
 * 
 */
public sealed class ConsoleUtility
{
    public static DuckDBConnection GenerateData(ExperimentConfig config)
    {
        // options available: "Data Source=file.db"; // "DataSource=:memory:"
        var connection = new DuckDBConnection(config.connectionString);
        connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = config.numCustomers,
            numProducts = config.runs[0].numProducts,
            numProdPerSeller = config.numProdPerSeller,
            qtyPerProduct = config.qtyPerProduct
        };
        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(connection);
        // dont need to generate customers on every run. only once
        dataGen.Generate(connection, true);
        GC.Collect();
        return connection;
    }

    public static ExperimentConfig BuildExperimentConfig_(string arg)
    {
        if (!File.Exists(arg))
        {
            Console.WriteLine("Configuration file passsed as parameter ({0}) does not exist", arg);
            throw new Exception("No file passed as parameter!");
        }

        Console.WriteLine("Init reading experiment configuration file...");
        ExperimentConfig experimentConfig;
        using (StreamReader r = new StreamReader(arg))
        {
            string json = r.ReadToEnd();
            Console.WriteLine("Configuration file contents:\n {0}", json);
            experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
        }
        Console.WriteLine("Experiment configuration read succesfully");
        return experimentConfig;
    }

    public static ExperimentConfig BuildExperimentConfig(string[] args)
    {
        if (args is not null && args.Length > 0)
        {
            Console.WriteLine("Directory of configuration files passsed as parameter: {0}", args[0]);
        } else
        {
            throw new Exception("No file passed as parameter!");
        }
        return BuildExperimentConfig_(args[0]);
    }

}
