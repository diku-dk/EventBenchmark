

using System.Collections.Generic;

namespace Common.Ingestion
{
    public class IngestionConfiguration
    {

        public DataSourceType dataNatureType;
        public IngestionPartitioningStrategy partitioningStrategy;
        public BackPressureStrategy backPressureStrategy;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus;

        public Dictionary<string, string> mapTableToUrl;

    }

}
