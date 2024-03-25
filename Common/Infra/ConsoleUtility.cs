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
    const char _block = '■';
    const string _back = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
    const string _twirl = "-\\|/";

    public static void WriteProgressBar(int percent, bool update = false)
    {
        if (update)
            Console.Write(_back);
        Console.Write("[");
        var p = (int)((percent / 10f) + .5f);
        for (var i = 0; i < 10; ++i)
        {
            if (i >= p)
                Console.Write(' ');
            else
                Console.Write(_block);
        }
        Console.Write("] {0,3:##0}%", percent);
    }

    public static void WriteProgress(int progress, bool update = false)
    {
        if (update)
            Console.Write("\b");
        Console.Write(_twirl[progress % _twirl.Length]);
    }

    public static DuckDBConnection GenerateData(ExperimentConfig config)
    {
        // "Data Source=file.db"; // "DataSource=:memory:"
        var connection = new DuckDBConnection(config.connectionString);
        connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = config.numCustomers,
            numProducts = config.runs[0].numProducts,
            numProdPerSeller = config.numProdPerSeller,
            qtyPerProduct = config.qtyPerProduct // fix bug, ohterwise it will be 0
        };
        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(connection);
        // dont need to generate customers on every run. only once
        dataGen.Generate(connection, true);
        GC.Collect();
        return connection;
    }

    public static ExperimentConfig BuildExperimentConfig(string[] args)
    {
        if (args is not null && args.Length > 0 && File.Exists(args[0]))
        {
            Console.WriteLine("Directory of configuration files passsed as parameter: {0}", args[0]);
        }
        else
        {
            throw new Exception("No file passed as parameter!");
        }

        Console.WriteLine("Init reading experiment configuration file...");
        ExperimentConfig experimentConfig;
        using (StreamReader r = new StreamReader(args[0]))
        {
            string json = r.ReadToEnd();
            Console.WriteLine("Configuration file contents:\n {0}", json);
            experimentConfig = JsonConvert.DeserializeObject<ExperimentConfig>(json);
        }
        Console.WriteLine("Experiment configuration read succesfully");

        return experimentConfig;

    }

}
