using Common.DataGeneration;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Ingestion;
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

    protected static readonly ILogger logger = LoggerProxy.GetInstance("WorkflowOrchestrator");

    protected static readonly List<TransactionType> eventualCompletionTransactions = new() { TransactionType.CUSTOMER_SESSION, TransactionType.PRICE_UPDATE, TransactionType.UPDATE_PRODUCT };

    public ExperimentManager(ExperimentConfig config)
    {
        this.config = config;
        this.connection = new DuckDBConnection(config.connectionString);
        connection.Open();
    }

    protected abstract void PreExperiment();

    protected abstract void PostExperiment();

    protected abstract void TrimStreams();

    protected abstract void PreWorkload(int runIdx);

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

        // cleanup microservice states
        var resps_ = new List<Task<HttpResponseMessage>>();
        foreach (var task in config.postExperimentTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Pre experiment task to URL {0}", task.url);
            resps_.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps_);

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

                var ingestionOrchestrator = new IngestionOrchestrator(config.ingestionConfig);
                await ingestionOrchestrator.Run(connection);

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

        var resps = new List<Task<HttpResponseMessage>>();
        foreach (var task in config.postExperimentTasks)
        {
            HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
            logger.LogInformation("Post experiment task to URL {0}", task.url);
            resps.Add(HttpUtils.client.SendAsync(message));
        }
        await Task.WhenAll(resps);
        logger.LogInformation("Post experiment cleanup tasks finished");

        logger.LogInformation("Experiment finished");
    }

    private async void PostRunTasks(int runIdx, int lastRunIdx)
    {
        // reset data in microservices - post run
        if (runIdx < lastRunIdx)
        {
            logger.LogInformation("Post run tasks started");
            var responses = new List<Task<HttpResponseMessage>>();
            List<PostRunTask> postRunTasks;
            // must call the cleanup if next run changes number of products
            if (config.runs[runIdx + 1].numProducts != config.runs[runIdx].numProducts)
            {
                logger.LogInformation("Next run changes the number of products.");
                postRunTasks = config.postExperimentTasks;
            }
            else
            {
                logger.LogInformation("Next run does not change the number of products.");
                postRunTasks = config.postRunTasks;
            }
            foreach (var task in postRunTasks)
            {
                HttpRequestMessage message = new HttpRequestMessage(HttpMethod.Patch, task.url);
                logger.LogInformation("Post run task to Microservice {0} URL {1}", task.name, task.url);
                responses.Add(HttpUtils.client.SendAsync(message));
            }
            await Task.WhenAll(responses);
            logger.LogInformation("Post run tasks finished");
        }

        logger.LogInformation("Run #{0} finished at {1}", runIdx, DateTime.UtcNow);

        logger.LogInformation("Memory used before collection:       {0:N0}",
                GC.GetTotalMemory(false));

        // Collect all generations of memory.
        GC.Collect();
        logger.LogInformation("Memory used after full collection:   {0:N0}",
        GC.GetTotalMemory(true));

    }

}
