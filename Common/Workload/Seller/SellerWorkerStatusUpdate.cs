using Common.Workload.Customer;

namespace Common.Workload.Seller
{
	public record SellerWorkerStatusUpdate
    (long sellerId, SellerWorkerStatus status);
}