using Common.Ingestion.Config;
using Orleans;
using Orleans.Concurrency;
using System.Threading.Tasks;

namespace GrainInterfaces.Ingestion
{
    public interface IIngestionOrchestrator : IGrainWithIntegerKey
    {

        Task Init(IngestionConfiguration config);

        // Task<bool> Run(IngestionConfiguration config);

        [AlwaysInterleave]
        Task<int> GetStatus();

    }
}
