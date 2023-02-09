using Common.Ingestion;
using Orleans;
using System.Threading.Tasks;

namespace GrainInterfaces.Ingestion
{
    public interface IIngestionOrchestrator : IGrainWithIntegerKey
    {

        Task<bool> Run(IngestionConfiguration config);

    }
}
