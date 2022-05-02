using Common;
using GrainInterfaces;
using Orleans;
using Orleans.Streams;
using System;
using System.Threading.Tasks;

namespace Grains
{
    public class PlayerGrain : Grain, IPlayerGrain
    {

        private int score;

        private readonly Random random;

        private IAsyncStream<PlayerUpdate> stream;

        public PlayerGrain()
        {
            score = 100;
            random = new Random();
        }

        public async override Task OnActivateAsync()
        {
            var streamProvider = GetStreamProvider("SMSProvider");
            stream = streamProvider.GetStream<PlayerUpdate>(Constants.playerUpdatesStreamId, Constants.streamNamespace);
            return;
        }

        public async Task PassToken(Token token)
        {
            int myId = (int)this.GetPrimaryKeyLong();

            Console.WriteLine($"I am [{myId}], received the token from [{token.lastPlayerID}] with rounds == [{token.rounds}].");

            int stolenPlayer = random.Next(0, Constants.NumberOfPlayers);
           
            while (myId == stolenPlayer)
            {
                // Avoid stealing points from the same player, since it would be meaningless
                stolenPlayer = (stolenPlayer + 1) % Constants.NumberOfPlayers;
            }
            
            IPlayerGrain stolenPlayerGrain = GrainFactory.GetGrain<IPlayerGrain>(stolenPlayer);

            int pointsToSteal = random.Next(1, 100);
 
            Task<int> taskSteal = stolenPlayerGrain.Steal(pointsToSteal);

            Console.WriteLine($"I am [{myId}], sent a steal request to [{stolenPlayer}] and the task result now is [{taskSteal.IsCompleted}].");

            // Now verify who is the next player to receive the token

            int toPlayer = random.Next(0, Constants.NumberOfPlayers);

            while (myId == toPlayer)
            {
                // Avoid transfering the token to the same player
                toPlayer = (toPlayer + 1) % Constants.NumberOfPlayers;
            }

            IPlayerGrain toPlayerGrain = GrainFactory.GetGrain<IPlayerGrain>(toPlayer);

            token.lastPlayerID = myId;
            token.rounds++;

            // Making sure theft is completed before proceeding
            int points = await taskSteal;

            Console.WriteLine($"I am [{myId}], I stole [{points}] from [{stolenPlayer}].");

            // increase my points
            score += points;

            Console.WriteLine($"I am [{myId}], sending an update to scoreboard. The points stolen by me were [{points}].");

            // update scoreboard
            PlayerUpdate playerUpdate = new PlayerUpdate(myId, points);
            _ = stream.OnNextAsync(playerUpdate);

            Console.WriteLine($"I am [{myId}], update sent to scoreboard.");

            Console.WriteLine($"I am [{myId}], now passing the token to [{toPlayer}].");

            await Task.Delay(TimeSpan.FromSeconds(1));

            _ = toPlayerGrain.PassToken(token);

            Console.WriteLine($"I am [{myId}], passed the token to [{toPlayer}].");

            return;
        }

        public Task<int> Steal(int points)
        {
            int myId = (int)this.GetPrimaryKeyLong();
            int pointsToReturn;

            Console.WriteLine($"I am [{myId}], received a stealing request.");

            if (points > score)
            {
                int aux = score;
                score = 0;
                pointsToReturn = aux;
            }
            else
            {
                score -= points;
                pointsToReturn = points;
            }

            Console.WriteLine($"I am [{myId}], request scoreboard update, [{pointsToReturn}] were stolen from me.");

            PlayerUpdate playerUpdate = new PlayerUpdate(myId, pointsToReturn * (-1));
            _ = stream.OnNextAsync(playerUpdate);

            Console.WriteLine($"I am [{myId}], scoreboard updated with stolen value.");

            return Task.FromResult( pointsToReturn );
        }
    }

}
