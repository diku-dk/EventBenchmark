using Common.Entities;
using Common.Workload.CustomerWorker;
using Common.Workload.Metrics;
using MathNet.Numerics.Distributions;

namespace Dapr.Workrs;
public sealed class CustomerThread
{  
    private readonly Random random;

    private CustomerWorkerConfig config;

    private IDiscreteDistribution sellerIdGenerator;

    // the customer this worker is simulating
    private int customerId;

    // the object respective to this worker
    private Customer customer;

    private readonly LinkedList<TransactionIdentifier> submittedTransactions;

    private readonly HttpClient httpClient;

    private readonly ILogger<CustomerThread> logger;



}