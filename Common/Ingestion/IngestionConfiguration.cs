

using System.Collections.Generic;
using Common.Ingestion.Worker;

namespace Common.Ingestion
{
    public class IngestionConfiguration
    {

        public DataSourceType dataNatureType;
        public IngestionPartitioningStrategy partitioningStrategy;
        public BackPressureStrategy? backPressureStrategy = BackPressureStrategy.NONE;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus;

        public Dictionary<string, string> mapTableToUrl;

    }

}
