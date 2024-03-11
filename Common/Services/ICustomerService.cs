using Common.Entities;
using Common.Streaming;
using Common.Workload.Metrics;

namespace Common.Services;

public interface ICustomerService
{
    void Run(int customerId, string tid);

    List<TransactionIdentifier> GetSubmittedTransactions(int customerId);

    List<TransactionOutput> GetFinishedTransactions(int customerId);

    List<TransactionMark> GetAbortedTransactions();

    void AddFinishedTransaction(int customerId, TransactionOutput transactionOutput);

    IDictionary<int, IDictionary<string,List<CartItem>>> GetCartHistoryPerCustomer(DateTime finishTime);
}

