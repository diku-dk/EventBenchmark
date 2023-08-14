using Common.Workload.Metrics;

namespace Daprr.Services;

public interface ICustomerService
{
    void Run(int customerId, int tid);

    List<TransactionIdentifier> GetSubmittedTransactions(int sellerId);
}


