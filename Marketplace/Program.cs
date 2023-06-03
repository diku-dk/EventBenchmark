using System;
using System.Threading.Tasks;
using Common.Http;
using Marketplace.Infra;
using Orleans;

namespace Marketplace
{
	public class Program
	{
        private static readonly ActorSettings defaultActorSettings = ActorSettings.GetDefault();

        public static async Task Main(string[] args)
        {
            IClusterClient client = await OrleansClientFactory.Connect();
            if (client == null) return;

            InitializePartitionedActors(client);

            // handler instance
            HttpHandler httpHandler = new HttpHandler(client);

			// initialize server
			HttpServer httpServer = new HttpServer(httpHandler);

            Task httpServerTask = Task.Run(httpServer.Run);

            Console.WriteLine("\n *************************************************************************");
            Console.WriteLine("            Marketplace started. Configuration:         ");
            Console.WriteLine("             numCustomerPartitions = {0}                ", defaultActorSettings.numCustomerPartitions);
            Console.WriteLine("             numOrderPartitions    = {0}                ", defaultActorSettings.numOrderPartitions);
            Console.WriteLine("             numPaymentPartitions  = {0}                ", defaultActorSettings.numPaymentPartitions);
            Console.WriteLine("             numProductPartitions  = {0}                ", defaultActorSettings.numProductPartitions);
            Console.WriteLine("             numShipmentPartitions = {0}                ", defaultActorSettings.numShipmentPartitions);
            Console.WriteLine("             numStockPartitions    = {0}                ", defaultActorSettings.numStockPartitions);
            Console.WriteLine("            Press any key to terminate...               ");
            Console.WriteLine("\n *************************************************************************");
            Console.ReadLine();
			httpServer.Stop();
            Task.WaitAll(httpServerTask, client.Close());
		}

        // configuration. number of partitions. let's start with one for each
        private static async void InitializePartitionedActors(IClusterClient client)
        {
            await client.GetGrain<IMetadataGrain>(0).Init(defaultActorSettings);
        }
    }
}

