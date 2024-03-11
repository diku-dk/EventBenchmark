using Common.Workload.Metrics;
using Common.Workers.Customer;
using Common.Streaming;
using Common.Entities;

namespace Common.Services;

public sealed class CustomerService : ICustomerService
{

    private readonly Dictionary<int, AbstractCustomerThread> customers;

    public CustomerService(Dictionary<int, AbstractCustomerThread> customers)
    {
        this.customers = customers;
    }

    public void Run(int customerId, string tid) => this.customers[customerId].Run(tid);

    public List<TransactionIdentifier> GetSubmittedTransactions(int sellerId)
    {
        return this.customers[sellerId].GetSubmittedTransactions();
    }

    public List<TransactionOutput> GetFinishedTransactions(int customerId)
    {
        return this.customers[customerId].GetFinishedTransactions();
    }

    public List<TransactionMark> GetAbortedTransactions()
    {
        List<TransactionMark> merged = new();
        foreach(var customer in this.customers)
        {
            merged.AddRange(customer.Value.GetAbortedTransactions());
        }
        return merged;
    }

    public void AddFinishedTransaction(int customerId, TransactionOutput transactionOutput)
    {
        this.customers[customerId].AddFinishedTransaction(transactionOutput);
    }

    public IDictionary<int,IDictionary<string,List<CartItem>>> GetCartHistoryPerCustomer(DateTime finishTime)
    {
        IDictionary<int, IDictionary<string, List<CartItem>>> dict = new Dictionary<int, IDictionary<string, List<CartItem>>>();
        foreach(int customerId in this.customers.Keys)
        {
            dict.Add(customerId, this.customers[customerId].GetCartItemsPerTid(finishTime) );
        }
        return dict;
    }
}

