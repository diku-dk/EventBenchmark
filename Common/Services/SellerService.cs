using Common.Entities;
using Common.Workload;
using Common.Workload.Metrics;
using Common.Workers.Seller;
using Common.Streaming;

namespace Common.Services;

public sealed class SellerService : ISellerService
{

    private readonly Dictionary<int, ISellerWorker> sellers;

    public SellerService(Dictionary<int, ISellerWorker> sellers)
    {
        this.sellers = sellers;
    }

    public Product GetProduct(int sellerId, int idx) => this.sellers[sellerId].GetProduct(idx);

    public List<TransactionOutput> GetFinishedTransactions(int sellerId)
    {
        return this.sellers[sellerId].GetFinishedTransactions();
    }

    public List<TransactionIdentifier> GetSubmittedTransactions(int sellerId)
    {
        return this.sellers[sellerId].GetSubmittedTransactions();
    }

    public void Run(int sellerId, string tid, TransactionType type) => sellers[sellerId].Run(tid, type);

    public List<TransactionMark> GetAbortedTransactions()
    {
        List<TransactionMark> merged = new();
        foreach(var seller in this.sellers)
        {
            merged.AddRange(seller.Value.GetAbortedTransactions());
        }
        return merged;
    }

    public void AddFinishedTransaction(int sellerId, TransactionOutput transactionOutput)
    {
        this.sellers[sellerId].AddFinishedTransaction(transactionOutput);
    }
}

