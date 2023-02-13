namespace Common.Ingestion.Worker
{
    public enum GroupRequestsStrategy
    {
        TASK_PER_REQUEST, // let the scheduler perform all requests individually
        TASK_PER_CPU, // divide the data across CPU number of tasks
        SINGLE_TASK, // all data from a batch is sent through a single task
    }
}
