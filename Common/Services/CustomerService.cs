using Common.Workload.Metrics;
using Common.Workers.Customer;
using Common.Streaming;
using Common.Entities;
using Common.Workload.CustomerWorker;

namespace Common.Services;

public sealed class CustomerService : ICustomerService
{
    public delegate AbstractCustomerWorker BuildCustomerWorkerDelegate(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer);

    // callback
    public readonly BuildCustomerWorkerDelegate BuildCustomerWorker;

    private readonly Dictionary<int, AbstractCustomerWorker> customers;

    public CustomerService(Dictionary<int, AbstractCustomerWorker> customers, BuildCustomerWorkerDelegate callback)
    {
        this.customers = customers;
        this.BuildCustomerWorker = callback;
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
            dict.Add(customerId, this.customers[customerId].GetCartItemsPerTid(finishTime));
        }
        return dict;
    }
}

