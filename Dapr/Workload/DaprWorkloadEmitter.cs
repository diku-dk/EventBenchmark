using Common.Distribution;
using Common.Workload;

namespace Dapr.Workload;

public class DaprWorkflowEmitter : WorkloadEmitter
{
    private readonly IHttpClientFactory httpClientFactory;

    public DaprWorkflowEmitter(
        IHttpClientFactory httpClientFactory,
        IDictionary<TransactionType, int> transactionDistribution,
        DistributionType sellerDistribution, Interval sellerRange,
        DistributionType customerDistribution, Interval customerRange,
        int concurrencyLevel, int executionTime, int delayBetweenRequests) :
        base(transactionDistribution, sellerDistribution, sellerRange, customerDistribution,
        customerRange, concurrencyLevel, executionTime, delayBetweenRequests)
    {
        this.httpClientFactory = httpClientFactory;

        // initialize all thread objects
        httpClientFactory.CreateClient();
    }

    protected override void SubmitTransaction(int tid, TransactionType type)
    {
        throw new NotImplementedException();
    }

}