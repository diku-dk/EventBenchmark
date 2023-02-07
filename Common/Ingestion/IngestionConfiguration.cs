

using System.Collections.Generic;

namespace Common.Ingestion
{
    public class IngestionConfiguration
    {

        public DataSourceType dataNatureType;
        public IngestionPartitioningStrategy partitioningStrategy;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus;

        public Dictionary<string, string> mapTableToUrl;

    }
}
