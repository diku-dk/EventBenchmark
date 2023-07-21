using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Common.Streaming;
using Orleans.Serialization;
using Microsoft.Extensions.DependencyInjection;

var builder = new HostBuilder()
   .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .ConfigureServices(services =>
            {
                services.AddHttpClient("default").ConfigurePrimaryHttpMessageHandler(() => new HttpClientHandler {
                    UseProxy = false,
                    Proxy = null,
                    UseCookies = false,
                    AllowAutoRedirect = false,
                    PreAuthenticate = false,
                    
                });
            })
            .UseLocalhostClustering()
            .AddMemoryStreams(StreamingConstants.DefaultStreamProvider)
            .AddMemoryGrainStorage(StreamingConstants.DefaultStreamStorage)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddConsole();
                logging.SetMinimumLevel(LogLevel.Warning);
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
