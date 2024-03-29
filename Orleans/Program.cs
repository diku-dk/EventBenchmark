﻿using Common.DataGeneration;
using Common.Experiment;
using DuckDB.NET.Data;
using Newtonsoft.Json;
using Orleans.Infra;
using Orleans.Workload;

namespace Orleans;

public class Program
{

    public static async Task Main(string[] args)
    {
        Console.WriteLine("Initializing benchmark driver...");
        ExperimentConfig config = BuildExperimentConfig(args);
        Console.WriteLine("Configuration parsed. Starting program...");
        DuckDBConnection connection = null;

        try{
        while(true){

        Console.WriteLine("\n Select an option: \n 1 - Generate Data \n 2 - Ingest Data \n 3 - Run Experiment \n 4 - Ingest and Run (2 and 3) \n 5 - Parse New Configuration \n q - Exit");
        string op = Console.ReadLine();

        switch (op)
        {
            case "1":
            {
                connection = GenerateData(config);
                break;
            }
            case "2":
            {
                if(connection is null){
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
                await CustomIngestionOrchestrator.Run(connection, config.ingestionConfig);
                break;
            }
            case "3":
            {
                if(connection is null) {
                    Console.WriteLine("Warning: Connection has not been set! Starting anyway...");
                }
                var expManager = new ActorExperimentManager(new CustomHttpClientFactory(), config, connection);
                await expManager.RunSimpleExperiment(2);
                break;
            }
            case "4":
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
                // ingest data
                await CustomIngestionOrchestrator.Run(connection, config.ingestionConfig);
                var expManager = new ActorExperimentManager(new CustomHttpClientFactory(), config, connection);
                // could be a config param: delay after ingest
                Console.WriteLine("Delay after ingest...");
                await Task.Delay(10000);
                // run
                await expManager.RunSimpleExperiment(2);
                Console.WriteLine("Experiment finished.");
                break;
            }
            case "5":
            {
                config = BuildExperimentConfig(args);
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
            Console.WriteLine("Exception catched. Source: {0}; Message: {0}", e.Source, e.StackTrace );
        }
    }

    private static DuckDBConnection GenerateData(ExperimentConfig config)
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
        if (args is not null && args.Length > 0 && File.Exists(args[0])) {
            Console.WriteLine("Directory of configuration files passsed as parameter: {0}", args[0]);
        } else
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


