using System;
using System.Collections.Generic;

namespace Client.Ingestion.Config
{
    public class IngestionConfig
    {

        public string connectionString { get; set; } = "Data Source=file.db"; // "DataSource=:memory:"

        // distribution of work strategy
        public IngestionDistributionStrategy distributionStrategy { get; set; } = IngestionDistributionStrategy.SINGLE_WORKER;

        // number of logical processors by default
        public readonly int concurrencyLevel = Environment.ProcessorCount;

        public IDictionary<string, string> mapTableToUrl;

    }

}
