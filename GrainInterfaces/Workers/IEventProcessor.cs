using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Workers
{

    /**
     * We can have multiple activations of this grain. It is a simply forwared
     * 
     */

    public interface IEventProcessor : IGrainWithIntegerKey
    {

        [AlwaysInterleave]
        Task ReceiveEvent(string value);

    }
}
