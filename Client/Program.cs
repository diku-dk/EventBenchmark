using Common;
using GrainInterfaces;
using Microsoft.Extensions.Logging;
using Orleans;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using var client = new ClientBuilder()
    .UseLocalhostClustering()
    //.ConfigureLogging(logging => logging.AddConsole())
    .Build();

await client.Connect();

// Create Token

Console.WriteLine($"Token generated");

var random = new Random();

int startGrainId = random.Next(0, Constants.NumberOfPlayers);

Console.WriteLine($"Starter player is [{startGrainId}]");

var starterGrain = client.GetGrain<IPlayerGrain>(startGrainId);

Token token = new Token(startGrainId);

IScoreboardGrain leaderboardGrain = client.GetGrain<IScoreboardGrain>(0);

// to force grain activation
await leaderboardGrain.Init();

await starterGrain.PassToken(token);

Console.WriteLine($"First token passed to player [{startGrainId}]");

await Task.Delay(TimeSpan.FromSeconds(5));

Dictionary<int, int> scoreboard = await leaderboardGrain.GetScoreboard();

Console.WriteLine($"The scoreboard after 10 seconds is []");

foreach (KeyValuePair<int, int> kvp in scoreboard)
{
    Console.WriteLine("Key = {0}, Value = {1}", kvp.Key, kvp.Value);
}

await client.Close();