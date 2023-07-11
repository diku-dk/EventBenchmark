namespace Client.Ingestion.Config
{
    public enum IngestionStrategy
    {
        SINGLE_WORKER,
        TABLE_PER_WORKER,
        WORKER_PER_CPU,
        WORKER_PER_ITEM
    }
}
