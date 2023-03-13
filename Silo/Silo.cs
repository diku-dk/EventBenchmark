using Orleans;
using Orleans.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using Common.Streaming;

var builder = new HostBuilder()
   .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            .AddMemoryGrainStorage(StreamingConfiguration.defaultStreamStorage)
            .AddSimpleMessageStreamProvider(StreamingConfiguration.defaultStreamProvider, options =>
            {
                options.PubSubType = Orleans.Streams.StreamPubSubType.ExplicitGrainBasedAndImplicit;
                options.FireAndForgetDelivery = false;
            })
            // .ConfigureLogging(logging => logging.ClearProviders())   //.AddSimpleConsole())
            .ConfigureLogging(logging =>
            {
                logging.SetMinimumLevel(LogLevel.Warning);
                //logging.ClearProviders();
                //logging.AddConsole();
            }) // https://learn.microsoft.com/en-us/aspnet/core/fundamentals/logging/?tabs=aspnetcore2x&view=aspnetcore-7.0
               // .UseDashboard(options => { })    // localhost:8080
        ;
    });

var server = builder.Build();
await server.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("            The Orleans server started. Press Enter to terminate...         ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await server.StopAsync();
