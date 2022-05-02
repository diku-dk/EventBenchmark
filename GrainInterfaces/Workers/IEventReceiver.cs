using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Workers
{
    public interface IEventReceiver : IGrainWithIntegerKey
    {

        Task ReceiveEvent(string value);

    }
}
