using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Workers
{
    public interface IMetadataService : IGrainWithIntegerKey
    {

        Task<string> RetrieveAssignedQueue(int actorId);

    }
}
