using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common.Ingestion.Worker;

namespace Common.Ingestion
{

    public class IngestionBatch
    {

        public string url;
     
        public List<string> data;

        public GroupRequestsStrategy? groupRequestsStrategy = GroupRequestsStrategy.TASK_PER_REQUEST;

    }
}
