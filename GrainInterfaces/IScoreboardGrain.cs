using System.Collections.Generic;
using Orleans;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    /// <summary>
    /// Interface to a leaderboard agent
    /// </summary>
    public interface IScoreboardGrain : IGrainWithIntegerKey
    {
        Task Init();
        Task<Dictionary<int, int>> GetScoreboard();
    }

}
