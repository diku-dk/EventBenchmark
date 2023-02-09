

namespace Common.Ingestion
{
    public enum IngestionPartitioningStrategy
    {
        SINGLE_WORKER, // one worker performs everything
        TABLE_PER_WORKER, // naive
        DISTRIBUTE_RECORDS_PER_NUM_CPUS // batch of records, containing type and url
    }
}
