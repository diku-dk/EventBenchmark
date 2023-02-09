using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common.Ingestion
{
    /**
     * Only for real data
     */
    public class IngestionBatch
    {

        public string url;
        public List<string> data;

        public BackPressureStrategy? backPressureStrategy;

    }
}
