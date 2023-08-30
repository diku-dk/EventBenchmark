using Common.Workload.Metrics;

namespace Common.Services;

public interface IDeliveryService
{

    void Run( int tid);

    List<(TransactionIdentifier, TransactionOutput)> GetResults();

}

