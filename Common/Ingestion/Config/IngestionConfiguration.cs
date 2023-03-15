using System;
using System.Collections.Generic;

namespace Common.Ingestion.Config
{
    public class IngestionConfiguration
    {

        public DataSourceType dataSourceType;

        // distribution of work startegy
        public IngestionDistributionStrategy distributionStrategy;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus = Environment.ProcessorCount;

        public Dictionary<string, string> mapTableToUrl;

    }

}
