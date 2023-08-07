using System.Collections.Generic;

namespace Common.Ingestion.Config
{
    public class IngestionConfig
    {

        public string connectionString { get; set; } //= "Data Source=file.db"; // "DataSource=:memory:"

        // distribution of work strategy
        public IngestionStrategy strategy { get; set; }

        // number of logical processors by default
        public int concurrencyLevel { get; set; }

        public IDictionary<string, string> mapTableToUrl;

    }

}
