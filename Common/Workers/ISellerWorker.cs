using Common.Distribution;
using Common.Entities;
using Common.Workload;
using Common.Workload.Metrics;

namespace Common.Workers
{
	public interface ISellerWorker
	{
		void Run(int tid, TransactionType type)
        {
            switch (type)
            {
                case TransactionType.QUERY_DASHBOARD:
                {
                    BrowseDashboard(tid);
                    break;
                }
                case TransactionType.UPDATE_PRODUCT:
                {
                    UpdateProduct(tid);
                    break;
                }
                case TransactionType.PRICE_UPDATE:
                {   
                    UpdatePrice(tid);
                    break;
                }
            }
        }

        void UpdatePrice(int tid);
        void UpdateProduct(int tid);
        void BrowseDashboard(int tid);

        List<TransactionIdentifier> GetSubmittedTransactions();
        List<TransactionOutput> GetFinishedTransactions();
        Product GetProduct(int idx);
        void SetUp(List<Product> products, DistributionType keyDistribution);
    }
}

