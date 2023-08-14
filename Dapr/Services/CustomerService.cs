using Common.Workload.Metrics;
using Daprr.Workrs;

namespace Daprr.Services;

public sealed class CustomerService : ICustomerService
{

    private readonly Dictionary<int, CustomerThread> customers;

    public CustomerService(Dictionary<int, CustomerThread> customers)
    {
        this.customers = customers;
    }

    public void Run(int customerId, int tid) => customers[customerId].Run(tid);

    public List<TransactionIdentifier> GetSubmittedTransactions(int sellerId)
    {
        return customers[sellerId].GetSubmittedTransactions();
    }

}

