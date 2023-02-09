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
            // .ConfigureLogging(logging => logging.ClearProviders())   //.AddSimpleConsole())
            .ConfigureLogging(logging =>
            {
                logging.SetMinimumLevel(LogLevel.Warning);
                //logging.ClearProviders();
                //logging.AddConsole();
            }) // https://learn.microsoft.com/en-us/aspnet/core/fundamentals/logging/?tabs=aspnetcore2x&view=aspnetcore-7.0
            ;
    })
    .RunConsoleAsync();
