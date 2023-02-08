using Orleans;
using Orleans.Hosting;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

await Host.CreateDefaultBuilder()
    .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            .AddMemoryGrainStorage("PubSubStore")
            .AddSimpleMessageStreamProvider("SMSProvider")
            .ConfigureLogging(logging => logging.ClearProviders())   //.AddSimpleConsole())
            ;
    })
    .RunConsoleAsync();
