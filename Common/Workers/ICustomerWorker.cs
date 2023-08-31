using Common.Workload.Metrics;

namespace Common.Workload.Workers;

public interface ICustomerWorker
{
    void Run(int tid);
    void AddItemsToCart();
    void Checkout(int tid);

    List<TransactionIdentifier> GetSubmittedTransactions();

}

