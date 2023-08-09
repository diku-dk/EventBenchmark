using Common.Entities;
using Common.Workload;

namespace Daprr.Services;

public interface ISellerService
{

    Product GetProduct(int sellerId);

    void Run(int sellerId, int tid, TransactionType type);

    bool HasAvailableProduct(int sellerId);

}