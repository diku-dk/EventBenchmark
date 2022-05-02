using Orleans;
using Orleans.Hosting;
using Microsoft.Extensions.Hosting;

await Host.CreateDefaultBuilder()
    .UseOrleans(siloBuilder =>
    {
        siloBuilder
            .UseLocalhostClustering()
            .AddMemoryGrainStorage("PubSubStore")
            .AddSimpleMessageStreamProvider("SMSProvider")
            ;
    })
    .RunConsoleAsync();
