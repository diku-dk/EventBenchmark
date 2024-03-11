using Common.Entities;
using Common.Streaming;
using Common.Workload;
using Common.Workload.Metrics;

namespace Common.Services;

public interface ISellerService
{
    Product GetProduct(int sellerId, int idx);

    void Run(int sellerId, string tid, TransactionType type);

    List<TransactionIdentifier> GetSubmittedTransactions(int sellerId);

    List<TransactionOutput> GetFinishedTransactions(int sellerId);

    List<TransactionMark> GetAbortedTransactions();

    void AddFinishedTransaction(int sellerId, TransactionOutput transactionOutput);

    IDictionary<int, List<Product>> GetTrackedProductUpdates();

}