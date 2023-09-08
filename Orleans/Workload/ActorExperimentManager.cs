using Common.Entities;
using System.Net.Http;
using Common.Experiment;
using Common.Infra;
using Common.Workload;
using Dapr.Workers;
using Common.Services;
using Common.Workers;
using Common.Workers.Seller;
using Dapr.Workload;
using Common.Ingestion;
using Microsoft.Extensions.Logging;
using Orleans.Workers;
using Common.Workers.Customer;

namespace Orleans.Workload;

public class ActorExperimentManager : ExperimentManager
{

    private readonly IHttpClientFactory httpClientFactory;

    private readonly SellerService sellerService;
    private readonly CustomerService customerService;
    private readonly DeliveryService deliveryService;

    private readonly Dictionary<int, ISellerWorker> sellerThreads;
    private readonly Dictionary<int, AbstractCustomerThread> customerThreads;
    private readonly DeliveryThread deliveryThread;

    private int numSellers;

    private ActorWorkloadManager workflowManager;

    public ActorExperimentManager(IHttpClientFactory httpClientFactory, ExperimentConfig config) : base(config)
    {
        this.httpClientFactory = httpClientFactory;

        this.deliveryThread = DeliveryThread.BuildDeliveryThread(httpClientFactory, config.deliveryWorkerConfig);
        this.deliveryService = new DeliveryService(this.deliveryThread);

        this.sellerThreads = new Dictionary<int, ISellerWorker>();
        this.sellerService = new SellerService(this.sellerThreads);
        this.customerThreads = new Dictionary<int, AbstractCustomerThread>();
        this.customerService = new CustomerService(this.customerThreads);

        this.numSellers = 0;
    }

    protected override void PreExperiment()
    {
        for (int i = this.customerRange.min; i <= this.customerRange.max; i++)
        {
            this.customerThreads.Add(i, ActorCustomerThread.BuildCustomerThread(httpClientFactory, sellerService, config.numProdPerSeller, config.customerWorkerConfig, customers[i-1]));
        }
    }

    protected override async void RunIngestion()
    {
        var ingestionOrchestrator = new IngestionOrchestrator(config.ingestionConfig);
        await ingestionOrchestrator.Run(connection);
    }

    protected override void PreWorkload(int runIdx)
    {
        this.numSellers = (int)DuckDbUtils.Count(connection, "sellers");

        for (int i = 1; i <= numSellers; i++)
        {
            List<Product> products = DuckDbUtils.SelectAllWithPredicate<Product>(connection, "products", "seller_id = " + i);
            if (!sellerThreads.ContainsKey(i))
            {
                sellerThreads[i] = ActorSellerThread.BuildSellerThread(i, httpClientFactory, config.sellerWorkerConfig);
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
            else
            {
                sellerThreads[i].SetUp(products, config.runs[runIdx].keyDistribution);
            }
        }

        Interval sellerRange = new Interval(1, this.numSellers);
        for (int i = customerRange.min; i <= customerRange.max; i++)
        {
            this.customerThreads[i].SetDistribution(this.config.runs[runIdx].sellerDistribution, sellerRange, this.config.runs[runIdx].keyDistribution);
        }

    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        throw new NotImplementedException();
    }

        protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        throw new NotImplementedException();
    }

    protected override void PostExperiment()
    {
        
    }

    protected override void PostRunTasks(int runIdx, int lastRunIdx)
    {
        logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

        logger.LogInformation("Memory used before collection:       {0:N0}",
                GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));
    }

    protected override void TrimStreams()
    {
        
    }
}