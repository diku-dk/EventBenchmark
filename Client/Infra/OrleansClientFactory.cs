using System;
using Common.Streaming;
using Orleans;
using Orleans.Runtime.Messaging;
using System.Threading.Tasks;
using Orleans.Hosting;
using Orleans.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;
using Common.Infra;

namespace Client.Infra
{
	public sealed class OrleansClientFactory
	{

        public static readonly TaskCompletionSource _siloFailedTask = new TaskCompletionSource();

        public static async Task<IClusterClient> Connect(int maxAttempts = int.MaxValue)
        {
            IClusterClient client;
            int attempts = 0;
            while (true)
            {
                client = new ClientBuilder()
                                .UseLocalhostClustering()
                                .Configure<GatewayOptions>(
                                    options =>                         // Default is 1 min.
                                    options.GatewayListRefreshPeriod = TimeSpan.FromMinutes(10)
                                )
                                .AddClusterConnectionLostHandler((x, y) =>
                                {
                                    LoggerProxy.GetInstance("ClusterConnectionLostHandler").LogCritical("Connection to cluster has been lost");
                                    _siloFailedTask.SetResult();
                                })
                                .ConfigureLogging(logging =>
                                {
                                    logging.ClearProviders();
                                    logging.AddConsole();
                                    logging.SetMinimumLevel(LogLevel.Warning);
                                })
                                .AddSimpleMessageStreamProvider(StreamingConstants.DefaultStreamProvider, options =>
                                {
                                    options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedOnly;
                                    options.FireAndForgetDelivery = false;
                                    options.OptimizeForImmutableData = true;
                                })
                                .Build();

                Func<Exception, Task<bool>> func = (x) =>
                {
                    return Task.FromResult(false);
                };

                try
                {
                    Task connectTask = client.Connect(func);
                    await connectTask;
                    break;
                }
                catch (ConnectionFailedException e)
                {
                    Console.Write("Error connecting to Silo: {0}.", e.Message);
                    attempts++;
                    if(attempts > maxAttempts)
                    {
                        throw;
                    }
                    Console.WriteLine("Trying again in 3 seconds...");
                    Thread.Sleep(TimeSpan.FromSeconds(3));
                }
            }
            return client;
        }

    }
}

