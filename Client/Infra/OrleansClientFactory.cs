﻿using Common.Streaming;
using Orleans.Runtime.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Serialization;

namespace Client.Infra
{
	public sealed class OrleansClientFactory
	{

        public static readonly TaskCompletionSource _siloFailedTask = new TaskCompletionSource();

        public static async Task<IClusterClient> Connect(int maxAttempts = int.MaxValue)
        {
            int attempts = 0;
            while (true)
            {

                var host = new HostBuilder()
                            .UseOrleansClient(
                                    client => client.UseLocalhostClustering()
                                    .AddMemoryStreams(StreamingConstants.DefaultStreamProvider)
                                    .AddClusterConnectionLostHandler((x,y) =>
                                    {
                                        Console.WriteLine("Connection to cluster has been lost");
                                        _siloFailedTask.SetResult();
                                    }).Services.AddSerializer(ser => {
                                        ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("Common"));
                                    })
                                )
                                .UseConsoleLifetime()
                                .Build();
                
                try
                {
                    await host.StartAsync();
                    return host.Services.GetRequiredService<IClusterClient>();
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
        }

    }
}

