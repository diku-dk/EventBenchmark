using Common.Streaming;
using Common.Workload.Metrics;

namespace Common.Services;

public interface ICustomerService
{
    void Run(int customerId, string tid);

    List<TransactionIdentifier> GetSubmittedTransactions(int sellerId);

    List<TransactionOutput> GetFinishedTransactions(int customerId);

    List<TransactionMark> GetAbortedTransactions();
}

