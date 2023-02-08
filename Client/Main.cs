using Common.Ingestion;
using GrainInterfaces.Ingestion;
using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Client
{

    /**
     * Based on http://sergeybykov.github.io/orleans/1.5/Documentation/Deployment-and-Operations/Docker-Deployment.html
     */
    public class Program
    {

        private static bool running;

        private static readonly IngestionConfiguration defaultIngestionConfig = new()
        {
            dataNatureType = DataSourceType.SYNTHETIC,
            partitioningStrategy = IngestionPartitioningStrategy.NONE,
            numberCpus = 2,
            mapTableToUrl = new Dictionary<string, string>()
            {
                ["test1"] = "127.0.0.1:8081/",
                ["test2"] = "127.0.0.1:8081/",
                ["warehouse"] = "127.0.0.1:8081/",
                ["districts"] = "127.0.0.1:8081/",
                ["items"] = "127.0.0.1:8081/",
                ["customers"] = "127.0.0.1:8081/",
                ["stockItems"] = "127.0.0.1:8081/",
            }
        };

        static void Main(string[] args)
        {
            Task.Run(() => InitializeOrleans());

            Console.ReadLine();

            running = false;
        }

        static async Task InitializeOrleans()
        {

            var client = await ConnectClient();
            
            var ingestionOrchestrator = client.GetGrain<IIngestionOrchestrator>(1);

            Console.WriteLine("Ingestion orchestrator grain obtained.");

            await ingestionOrchestrator.Run(defaultIngestionConfig);

            Console.WriteLine("Ingestion orchestrator grain finished.");

            // TODO setup grains with default or provided config

            // TODO bulk data ingestor grain... maybe not necessary now, just a thread pool with a thread per microservice....

            // setup rabbitmq client after generating the data

            await client.Close();
        }

        public static async Task<IClusterClient> ConnectClient()
        {
            Console.WriteLine("Initializing...");

            var client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .Build();
            await client.Connect();

            running = true;
            Console.WriteLine("Initialized!");

            return client;
        }

    }

}
