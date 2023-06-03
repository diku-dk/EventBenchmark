using System;
using System.Collections.Generic;

namespace Common.Ingestion.Config
{
    public class IngestionConfiguration
    {

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        // distribution of work strategy
        public IngestionDistributionStrategy distributionStrategy = IngestionDistributionStrategy.SINGLE_WORKER;

        // number of logical processors = Environment.ProcessorCount
        public int numberCpus = Environment.ProcessorCount;

        public IDictionary<string, string> mapTableToUrl;

    }

}
