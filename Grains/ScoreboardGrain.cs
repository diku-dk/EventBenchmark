using Common;
using GrainInterfaces;
using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Grains
{
    public class ScoreboardGrain : Grain, IScoreboardGrain
    {
        private readonly Dictionary<int, int> scoreboard;

        public ScoreboardGrain()
        {
            scoreboard = new Dictionary<int, int>();
        }

        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            var stream = streamProvider.GetStream<PlayerUpdate>(Constants.playerUpdatesStreamId, Constants.streamNamespace);
            await stream.SubscribeAsync(UpdatePlayerScoreAsync);
        }

        public Task Init()
        {
            return Task.CompletedTask;
        }

        private Task UpdatePlayerScoreAsync(PlayerUpdate item, StreamSequenceToken token = null)
        {
            int currentScore;
            if(scoreboard.ContainsKey(item.playerId))
            {
                scoreboard.TryGetValue(item.playerId, out currentScore);
                Console.WriteLine($"Player [{item.playerId}] already in scoreboard with score [{currentScore}].");
                currentScore += item.update;
                scoreboard[item.playerId] = currentScore;
                Console.WriteLine($"Player [{item.playerId}] score updated to [{currentScore}].");

            } else
            {
                currentScore = 100 + item.update;
                scoreboard.Add(item.playerId, currentScore);
                Console.WriteLine($"Player [{item.playerId}] not in scoreboard. Starting score is [{currentScore}].");
            }
            return Task.CompletedTask;
        }

        public async Task<Dictionary<int,int>> GetScoreboard() => await Task.FromResult(scoreboard);

    }

}
