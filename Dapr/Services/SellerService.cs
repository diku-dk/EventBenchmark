using Common.Entities;
using Common.Workload;
using Daprr.Workers;

namespace Daprr.Services;

public sealed class SellerService : ISellerService
{
    
    private Dictionary<int, SellerThread>? sellers { get; set; }

    public SellerService(){ }

    public Product GetProduct(int sellerId) => sellers[sellerId].GetProduct();

    public void Run(int sellerId, int tid, TransactionType type) => sellers[sellerId].Run(tid, type);

    public bool HasAvailableProduct(int sellerId) => sellers[sellerId].HasAvailableProducts();

}

