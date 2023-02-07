using Common.Ingestion;
using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GrainInterfaces.Ingestion
{
    public interface IIngestionOrchestrator : IGrainWithIntegerKey
    {

        public Task Run(IngestionConfiguration config);

    }
}
