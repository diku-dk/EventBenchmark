using Common;
using Orleans;
using System.Threading.Tasks;

namespace GrainInterfaces
{
    /// <summary>
    /// Interface to an individual player
    /// </summary>
    public interface IPlayerGrain : IGrainWithIntegerKey
    {
        Task PassToken(Token token);
        Task<int> Steal(int points);
    }

}
