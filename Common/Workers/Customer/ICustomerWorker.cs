using Common.Distribution;
using Common.Workload;
using Common.Workload.Metrics;

namespace Common.Workers.Customer;

public interface ICustomerWorker
{
    void Run(int tid);
    void AddItemsToCart();
    void Checkout(int tid);

    List<TransactionIdentifier> GetSubmittedTransactions();

    // only for Orleans
    List<TransactionOutput> GetFinishedTransactions();

    abstract void SetUp(DistributionType sellerDistribution, Interval sellerRange, DistributionType keyDistribution);

}

