using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Common.Streaming;
using Orleans.Providers;
using Orleans.Serialization;

var builder = new HostBuilder()
   .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            .AddMemoryStreams<DefaultMemoryMessageBodySerializer>(StreamingConstants.DefaultStreamProvider,_ =>
            {
                _.ConfigurePartitioning(8);
            })
            .AddMemoryGrainStorage(StreamingConstants.DefaultStreamStorage)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Information);
            }).Services.AddSerializer(ser => {
                ser.AddNewtonsoftJsonSerializer(isSupported: type => type.Namespace.StartsWith("Common"));
            })
        ;
    });

var server = builder.Build();
await server.StartAsync();
Console.WriteLine("\n *************************************************************************");
Console.WriteLine("            The Orleans silo started. Press any key to terminate...         ");
Console.WriteLine("\n *************************************************************************");
Console.ReadLine();
await server.StopAsync();
