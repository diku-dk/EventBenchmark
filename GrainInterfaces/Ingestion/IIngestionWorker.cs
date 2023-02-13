using Common.Ingestion;
using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GrainInterfaces.Ingestion
{
    public interface IIngestionWorker : IGrainWithStringKey
    {

        public Task Send(IngestionBatch batch);

        public Task Send(List<IngestionBatch> batches);

    }
}
