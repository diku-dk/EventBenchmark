using Common.Streaming;
using Common.Workload.Metrics;

namespace Common.Workers.Delivery;

public interface IDeliveryWorker
{
	void Run(string tid);

	List<TransactionMark> GetAbortedTransactions();

	List<(TransactionIdentifier, TransactionOutput)> GetResults();

	void AddFinishedTransaction(TransactionOutput transactionOutput);

	List<TransactionIdentifier> GetSubmittedTransactions();

	List<TransactionOutput> GetFinishedTransactions();
}

