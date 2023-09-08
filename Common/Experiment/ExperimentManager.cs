using Common.DataGeneration;
using Common.Entities;
using Common.Infra;
using Common.Workload;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace Common.Experiment;

public abstract class ExperimentManager
{

    protected readonly ExperimentConfig config;
    protected readonly DuckDBConnection connection;
    protected List<Customer> customers;
    protected Interval customerRange;

    protected static readonly byte ITEM = 0;

    protected static readonly ILogger logger = LoggerProxy.GetInstance("ExperimentManager");

    protected static readonly List<TransactionType> eventualCompletionTransactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.UPDATE_PRODUCT };

    public ExperimentManager(ExperimentConfig config)
    {
        this.config = config;
        this.connection = new DuckDBConnection(config.connectionString);
        connection.Open();
    }

    protected abstract void PreExperiment();

    protected abstract void RunIngestion();

    protected abstract void PostExperiment();

    protected abstract void TrimStreams();

    protected abstract void PreWorkload(int runIdx);

    protected abstract void PostRunTasks(int runIdx, int lastRunIdx);

    protected abstract WorkloadManager SetUpManager(int runIdx);

    protected abstract void Collect(int runIdx, DateTime startTime, DateTime finishTime);

    public async Task Run()
    {
        
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = config.numCustomers,
            numProducts = 0 // to force product generation and ingestion in the upcoming loop
        };

        int runIdx = 0;
        int lastRunIdx = config.runs.Count() - 1;

        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(connection);
        // dont need to generate customers on every run. only once
        dataGen.GenerateCustomers(connection);

        // customers are fixed accross runs
        this.customers = DuckDbUtils.SelectAll<Customer>(connection, "customers");
        this.customerRange = new Interval(1, config.numCustomers);

        PreExperiment();

        foreach (var run in config.runs)
        {
            logger.LogInformation("Run #{0} started at {0}", runIdx, DateTime.UtcNow);

            if (run.numProducts != previousData.numProducts)
            {
                logger.LogInformation("Run #{0} number of products changed from last run {0}", runIdx, runIdx - 1);

                // update previous
                previousData = new SyntheticDataSourceConfig()
                {
                    connectionString = config.connectionString,
                    numProdPerSeller = config.numProdPerSeller,
                    numCustomers = config.numCustomers,
                    numProducts = run.numProducts
                };
                var syntheticDataGenerator = new SyntheticDataGenerator(previousData);

                // must truncate if not first run
                if (runIdx > 0)
                    syntheticDataGenerator.TruncateTables(connection);

                syntheticDataGenerator.Generate(connection);

                RunIngestion();

                if (runIdx == 0)
                {
                    // remove customers from ingestion config from now on
                    config.ingestionConfig.mapTableToUrl.Remove("customers");
                }

            }

            PreWorkload(runIdx);

            WorkloadManager workloadManager = SetUpManager(runIdx);

            var workloadTask = await workloadManager.Run();

            DateTime startTime = workloadTask.startTime;
            DateTime finishTime = workloadTask.finishTime;

            logger.LogInformation("Wait for microservices to converge (i.e., finish receiving events) for {0} seconds...", config.delayBetweenRuns / 1000);
            await Task.Delay(config.delayBetweenRuns);

            // set up data collection for metrics
            Collect(runIdx, startTime, finishTime);

            // trim first to avoid receiving events after the post run task
            TrimStreams();

            PostRunTasks(runIdx, lastRunIdx);

            // increment run index
            runIdx++;
        }

        logger.LogInformation("Post experiment cleanup tasks started.");

        PostExperiment();

        logger.LogInformation("Experiment finished");
    }

}
