using Common.Workload.Metrics;
using Common.Workers.Customer;
using Common.Entities;

namespace Common.Services;

public sealed class CustomerService : ICustomerService
{

    private readonly Dictionary<int, AbstractCustomerThread> customers;

    public CustomerService(Dictionary<int, AbstractCustomerThread> customers)
    {
        this.customers = customers;
    }

    public void Run(int customerId, int tid) => customers[customerId].Run(tid);

    public List<TransactionIdentifier> GetSubmittedTransactions(int sellerId)
    {
        return customers[sellerId].GetSubmittedTransactions();
    }

    public List<TransactionOutput> GetFinishedTransactions(int sellerId)
    {
        return customers[sellerId].GetFinishedTransactions();
    }
}

