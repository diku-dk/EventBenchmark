using Common.DataGeneration;
using Common.Entities;
using Common.Infra;
using Common.Ingestion;
using Common.Workload;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace Common.Experiment;

public abstract class AbstractExperimentManager
{
    protected readonly ExperimentConfig config;
    protected readonly DuckDBConnection connection;
    protected List<Customer> customers;
    protected readonly Interval customerRange;

    protected static readonly byte ITEM = 0;

    protected static readonly ILogger logger = LoggerProxy.GetInstance("ExperimentManager");

    protected static readonly List<TransactionType> eventualCompletionTransactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.UPDATE_PRODUCT };

    public AbstractExperimentManager(ExperimentConfig config, DuckDBConnection duckDBConnection)
    {
        this.config = config;
        this.customerRange = new Interval(1, config.numCustomers);
        this.connection = duckDBConnection;
    }

    protected abstract void PreExperiment();

    protected virtual void PostExperiment() { }

    protected virtual void TrimStreams() { }

    protected abstract void PreWorkload(int runIdx);

    protected virtual void PostRunTasks(int runIdx, int lastRunIdx) { }

    protected abstract WorkloadManager SetUpManager(int runIdx);

    protected abstract void Collect(int runIdx, DateTime startTime, DateTime finishTime);

    public virtual async Task Run()
    {
        this.connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = this.config.numCustomers,
            numProducts = 0 // to force product generation and ingestion in the upcoming loop
        };

        int runIdx = 0;
        int lastRunIdx = this.config.runs.Count() - 1;

        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(this.connection);
        // dont need to generate customers on every run. only once
        dataGen.GenerateCustomers(this.connection);
        // customers are fixed accross runs
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");

        this.PreExperiment();

        foreach (var run in this.config.runs)
        {
            logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.UtcNow);

            if (run.numProducts != previousData.numProducts)
            {
                logger.LogInformation("Run #{0} number of products changed from last run {0}", runIdx, runIdx - 1);

                // update previous
                previousData = new SyntheticDataSourceConfig()
                {
                    numProdPerSeller = this.config.numProdPerSeller,
                    numCustomers = this.config.numCustomers,
                    numProducts = run.numProducts,
                    qtyPerProduct = this.config.qtyPerProduct
                };
                var syntheticDataGenerator = new SyntheticDataGenerator(previousData);

                // must truncate if not first run
                if (runIdx > 0)
                    syntheticDataGenerator.TruncateTables(connection);

                syntheticDataGenerator.Generate(connection);

                await IngestionOrchestrator.Run(connection, config.ingestionConfig);
    
                if (runIdx == 0)
                {
                    // remove customers from ingestion config from now on
                    this.config.ingestionConfig.mapTableToUrl.Remove("customers");
                }

            }

            this.PreWorkload(runIdx);

            WorkloadManager workloadManager = this.SetUpManager(runIdx);

            logger.LogInformation("Run #{0} started at {1}", runIdx, DateTime.UtcNow);

            var workloadTask = await workloadManager.Run();

            DateTime startTime = workloadTask.startTime;
            DateTime finishTime = workloadTask.finishTime;

            logger.LogInformation("Wait for microservices to converge (i.e., finish receiving events) for {0} seconds...", this.config.delayBetweenRuns / 1000);
            await Task.Delay(this.config.delayBetweenRuns);

            // set up data collection for metrics
            this.Collect(runIdx, startTime, finishTime);

            // trim first to avoid receiving events after the post run task
            this.TrimStreams();

            this.CollectGarbage();

            logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

            // increment run index
            runIdx++;

            if(runIdx < (config.runs.Count - 1))
                this.PostRunTasks(runIdx, lastRunIdx);
        }

        logger.LogInformation("Post experiment cleanup tasks started.");

        this.PostExperiment();

        logger.LogInformation("Experiment finished");
    }

    public async Task RunSimpleExperiment()
    {
        this.customers = DuckDbUtils.SelectAll<Customer>(this.connection, "customers");
        this.PreExperiment();
        this.PreWorkload(0);
        var workloadManager = this.SetUpManager(0);
        (DateTime startTime, DateTime finishTime) res = await workloadManager.Run();
        DateTime startTime = res.startTime;
        DateTime finishTime = res.finishTime;
        this.Collect(0, startTime, finishTime);
        this.PostExperiment();
        this.CollectGarbage();
    }

    protected void CollectGarbage()
    {
        logger.LogInformation("Memory used before collection:       {0:N0}",
        GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));
    }

}
