namespace Common.Workload.Customer
{
    public record CustomerWorkerStatusUpdate(long customerId, CustomerWorkerStatus status);
}