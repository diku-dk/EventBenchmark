namespace Common.Ingestion.Config
{
    public class IngestionConfig
    {

        public string connectionString { get; set; } //= "Data Source=file.db"; // "DataSource=:memory:"

        // distribution of work strategy
        public IngestionStrategy strategy { get; set; } = IngestionStrategy.SINGLE_WORKER;

        // number of logical processors by default
        public int concurrencyLevel { get; set; } = 1;

        public IDictionary<string, string> mapTableToUrl;

    }

}
