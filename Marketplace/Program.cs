using System;
using System.Threading.Tasks;
using Common.Http;
using Marketplace.Infra;

namespace Marketplace
{
	public class Program
	{

        // configuration. number of partitions. let's start with one for each

        // FIXME: Unhandled exception. System.ArgumentException:
        // Cannot find an implementation class for grain interface: Marketplace.Actor.IProductActor.
        // Make sure the grain assembly was correctly deployed and loaded in the silo.
        // probably need to put the interfaces in a different class file

        public static async Task Main(string[] args)
        {
            var client = await OrleansClientFactory.Connect();
            if (client == null) return;

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
	}
}

