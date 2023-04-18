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
            Console.WriteLine("            Marketplace started. Press any key to terminate...         ");
            Console.WriteLine("\n *************************************************************************");
            Console.ReadLine();
			httpServer.Stop();
		}

        // configuration. number of partitions. let's start with one for each
        private static async void InitializePartitionedActors(IClusterClient client)
        {
            await client.GetGrain<IMetadataGrain>(0).Init(defaultActorSettings);
        }
    }
}

