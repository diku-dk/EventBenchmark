using Common.Workload.Metrics;

namespace Daprr.Services;

public interface IDeliveryService
{

    void Run( int tid);

    List<(TransactionIdentifier, TransactionOutput)> GetResults();

}

