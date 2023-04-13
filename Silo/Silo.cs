using Orleans;
using Orleans.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using Common.Streaming;
using System.Net;

// https://learn.microsoft.com/en-us/dotnet/orleans/host/configuration-guide/typical-configurations
// const string PRIMARY_SILO_IP_ADDRESS = "127.0.0.1";
// IPEndPoint primarySiloEndpoint = new IPEndPoint(PRIMARY_SILO_IP_ADDRESS, 11111);
// var primarySiloEndpoint = new IPEndpoint(PRIMARY_SILO_IP_ADDRESS, 11111);

var builder = new HostBuilder()
   .UseOrleans(siloBuilder =>
    {
        
        siloBuilder
            .UseLocalhostClustering()
            //.UseDevelopmentClustering(primarySiloEndpoint)
            .AddMemoryGrainStorage(StreamingConfiguration.DefaultStreamStorage)
            .AddSimpleMessageStreamProvider(StreamingConfiguration.DefaultStreamProvider, options =>
            {
                options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedOnly;
                options.FireAndForgetDelivery = false;
                options.OptimizeForImmutableData = true; // to pass by reference, saving costs
            })
            // .ConfigureLogging(logging => logging.ClearProviders())   //.AddSimpleConsole())
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Warning);
            }) // https://learn.microsoft.com/en-us/aspnet/core/fundamentals/logging/?tabs=aspnetcore2x&view=aspnetcore-7.0
               // .UseDashboard(options => { })    // localhost:8080
               // .ConfigureServices(services => { services.Add })
        ;
    });

var server = builder.Build();
await server.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("            The Orleans silo started. Press any key to terminate...         ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await server.StopAsync();
