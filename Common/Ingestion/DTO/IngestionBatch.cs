using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Ingestion.Config;

namespace Common.Ingestion.DTO
{

    public class IngestionBatch
    {

        public string url;
     
        public List<string> data;

        public BatchRequestsStrategy? groupRequestsStrategy = BatchRequestsStrategy.TASK_PER_REQUEST;

    }
}
