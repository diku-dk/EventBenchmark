using Common.Entities;
using Common.Workload;
using Common.Workload.Metrics;

namespace Common.Services;

public interface ISellerService
{
    Product GetProduct(int sellerId, int idx);

    void Run(int sellerId, string tid, TransactionType type);

    List<TransactionIdentifier> GetSubmittedTransactions(int sellerId);
    List<TransactionOutput> GetFinishedTransactions(int sellerId);
}