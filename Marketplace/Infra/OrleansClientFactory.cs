using System;
using Common.Streaming;
using Orleans;
using Orleans.Runtime.Messaging;
using System.Threading.Tasks;
using Orleans.Hosting;
using Orleans.Configuration;
using Microsoft.Extensions.Logging;

namespace Marketplace.Infra
{
	public sealed class OrleansClientFactory
	{
        public static async Task<IClusterClient> Connect()
        {
            IClusterClient client = new ClientBuilder()
                                .UseLocalhostClustering()
                                .Configure<GatewayOptions>(
                                    options =>
                                    options.GatewayListRefreshPeriod = TimeSpan.FromMinutes(10))
                                .ConfigureLogging(logging =>
                                {
                                    logging.ClearProviders();
                                    logging.AddConsole();
                                    logging.SetMinimumLevel(LogLevel.Warning);
                                })
                                .AddSimpleMessageStreamProvider(StreamingConfiguration.DefaultStreamProvider, options =>
                                {
                                    options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedOnly;
                                    options.FireAndForgetDelivery = false;
                                    options.OptimizeForImmutableData = true;
                                })
                                .Build();

            Func<Exception, Task<bool>> func = (x) => {
                return Task.FromResult(false);
            };

            try
            {
                Task connectTask = client.Connect(func);
                await connectTask;
                return client;
            }
            catch (ConnectionFailedException e)
            {
                Console.WriteLine("Error connecting to Silo: {0}", e.Message);
            }
            return null;
        }

    }
}

