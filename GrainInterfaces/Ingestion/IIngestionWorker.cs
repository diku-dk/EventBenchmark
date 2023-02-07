using Common.Ingestion;
using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GrainInterfaces.Ingestion
{
    public interface IIngestionWorker : IGrainWithStringKey
    {

        public Task Send(IngestionBatch batch);

        public Task Send(List<IngestionBatch> batches);

    }
}
