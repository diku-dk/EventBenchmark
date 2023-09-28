using Common.Workload.Metrics;

namespace Common.Services;

public interface IDeliveryService
{

    void Run(string tid);

    List<(TransactionIdentifier, TransactionOutput)> GetResults();

}

