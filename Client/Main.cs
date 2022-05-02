using Common;
using GrainInterfaces;
using Microsoft.Extensions.Logging;
using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Client
{
    public class Program
    {

        public static int Main(string[] args)
        {
            return RunMainAsync().Result;
        }

        private static async Task<int> RunMainAsync()
        {

            using (var client = await ConnectClient())
            {

                // Create Token
                Token token = new Token();

                Console.WriteLine($"Token generated");

                var random = new Random();

                int startGrainId = random.Next(1, Constants.NumberOfPlayers);

                Console.WriteLine($"Starter player is [{startGrainId}]");

                var starterGrain = client.GetGrain<IPlayerGrain>(startGrainId);

                // Console.WriteLine($"Toek event(s) to StreamId: [{sourceAccountLogId}, {outgoingStreamNamespace}]");

                _ = starterGrain.PassToken(token);

                Console.WriteLine($"First token passed! to player [{startGrainId}]");

                await Task.Delay(TimeSpan.FromSeconds(10));

                IScoreboardGrain leaderboardGrain = client.GetGrain<IScoreboardGrain>(0);

                Dictionary<int, int> scoreboard = await leaderboardGrain.GetScoreboard();

                Console.WriteLine($"The scoreboard after 10 seconds is [{scoreboard}]");
                
            }

            return 0;
        }

        public static async Task<IClusterClient> ConnectClient()
        {
            using var client = new ClientBuilder()
                                .UseLocalhostClustering()
                                //.ConfigureLogging(logging => logging.AddConsole())
                                .Build();
            await client.Connect();
            return client;
        }

    }

}
