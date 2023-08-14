using Common.Infra;
using Common.Workload;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Common.Metric;

public abstract class MetricManager
{

    protected static readonly ILogger logger = LoggerProxy.GetInstance("MetricGather");

    public MetricManager()
	{
	}

    protected List<Latency> BuildLatencyList(Dictionary<int, TransactionIdentifier> submitted, Dictionary<int, TransactionOutput> finished, DateTime finishTime, string workerType = "")
    {
        var targetValues = finished.Values.Where(e => e.timestamp.CompareTo(finishTime) <= 0);
        var latencyList = new List<Latency>(targetValues.Count());
        foreach (var entry in targetValues)
        {
            if (!submitted.ContainsKey(entry.tid))
            {
                logger.LogWarning("[{0}] Cannot find correspondent submitted TID from finished transaction {0}", workerType, entry);
                continue;
            }
            var init = submitted[entry.tid];
            var latency = (entry.timestamp - init.timestamp).TotalMilliseconds;
            if (latency < 0)
            {
                logger.LogWarning("[{0}] Negative latency found for TID {1}. Init {2} End {3}", workerType, entry.tid, init, entry);
                continue;
            }
            latencyList.Add(new Latency(entry.tid, init.type, latency, entry.timestamp));

        }
        return latencyList;
    }

    public void Collect(DateTime startTime, DateTime finishTime, int epochPeriod = 0, string runName = null)
    {

        logger.LogInformation("[MetricGather] Starting collecting metrics for run between {0} and {1}", startTime, finishTime);

        StreamWriter sw;
        if (runName is not null)
        {
            sw = new StreamWriter(string.Format("{0}.txt", runName));
        }
        else
        {
            string unixTimeMilliSeconds = new DateTimeOffset(startTime).ToUnixTimeMilliseconds().ToString();
            sw = new StreamWriter(string.Format("results_{0}.txt", unixTimeMilliSeconds));
        }

        sw.WriteLine("Run from {0} to {1}", startTime, finishTime);
        sw.WriteLine("===========================================");

        // seller workers
        List<Latency> sellerLatencyList = CollectFromSeller(finishTime);
        // customer workers
        List<Latency> customerLatencyList = CollectFromCustomer(finishTime);
        // delivery worker
        List<Latency> deliveryLatencyList = CollectFromDelivery(finishTime);

        var latencyGatherResults = new List<List<Latency>>
        {
            sellerLatencyList,
            customerLatencyList,
            deliveryLatencyList
        };

        Dictionary<TransactionType, List<double>> latencyCollPerTxType = new();

        var txTypeValues = Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>().ToList();
        foreach (var txType in txTypeValues)
        {
            latencyCollPerTxType.Add(txType, new List<double>());
        }

        int countTid = 0;
        foreach (var list in latencyGatherResults)
        {
            foreach (var entry in list)
            {
                latencyCollPerTxType[entry.type].Add(entry.totalMilliseconds);
            }
            countTid += list.Count;
        }

        foreach (var entry in latencyCollPerTxType)
        {
            double avg = 0;
            if (entry.Value.Count > 0)
            {
                avg = entry.Value.Average();
            }
            logger.LogInformation("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
            sw.WriteLine("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
        }

        // transactions per second
        TimeSpan executionTime = finishTime - startTime;
        double txPerSecond = countTid / executionTime.TotalSeconds;

        logger.LogInformation("Number of seconds: {0}", executionTime.TotalSeconds);
        sw.WriteLine("Number of seconds: {0}", executionTime.TotalSeconds);
        logger.LogInformation("Number of completed transactions: {0}", countTid);
        sw.WriteLine("Number of completed transactions: {0}", countTid);
        logger.LogInformation("Transactions per second: {0}", txPerSecond);
        sw.WriteLine("Transactions per second: {0}", txPerSecond);
        sw.WriteLine("=====================================================");

        if (epochPeriod <= 0)
        {
            logger.LogWarning("Epoch period <= 0. No breakdown will be calculated!");
            goto end_metric;
        }

        // TODO calculate percentiles
        // break down latencies by end timestamp
        int blocks = (int)executionTime.TotalMilliseconds / epochPeriod;
        logger.LogInformation("{0} blocks for epoch {1}", blocks, epochPeriod);
        sw.WriteLine("{0} blocks for epoch {1}", blocks, epochPeriod);

        List<Dictionary<TransactionType, List<double>>> breakdown = new(blocks);
        for (int i = 0; i < blocks; i++)
        {
            breakdown.Add(new());
            foreach (var txType in txTypeValues)
            {
                breakdown[i].Add(txType, new List<double>());
            }
        }
        // iterate over all results and 
        foreach (var list in latencyGatherResults)
        {
            foreach (var entry in list)
            {
                // find the block the entry belongs to
                var span = entry.endTimestamp.Subtract(startTime);
                int idx = (int)(span.TotalMilliseconds / epochPeriod);
                if (idx < 0 || idx >= breakdown.Count)
                {
                    logger.LogWarning("Entry outside breakdown boundary. Finish time is {0} and the entry is {1}", finishTime, entry);
                    continue;
                }
                breakdown[idx][entry.type].Add(entry.totalMilliseconds);
            }
        }

        int blockIdx = 1;
        foreach (var block in breakdown)
        {

            logger.LogInformation("Block {0} results:", blockIdx);
            sw.WriteLine("Block {0} results:", blockIdx);

            // iterating over each transaction type in block
            int blockCountTid = 0;
            foreach (var entry in block)
            {
                double avg = 0;
                if (entry.Value.Count > 0)
                {
                    avg = entry.Value.Average();
                }
                blockCountTid += entry.Value.Count;
                logger.LogInformation("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
                sw.WriteLine("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, avg.ToString());
            }

            double blockTxPerSecond = blockCountTid / (epochPeriod / 1000);

            logger.LogInformation("Number of completed transactions: {0}", blockCountTid);
            sw.WriteLine("Number of completed transactions: {0}", blockCountTid);
            logger.LogInformation("Transactions per second: {0}", blockTxPerSecond);
            sw.WriteLine("Transactions per second: {0}", blockTxPerSecond);

            blockIdx++;
            sw.WriteLine("===========================================");

        }

        sw.WriteLine("================== Aborts ==================");
        Dictionary<TransactionType, int> dictCount = CollectAborts(finishTime);
        foreach (var entry in dictCount)
        {
              sw.WriteLine("Transaction: {0} - Average end-to-end latency: {1}", entry.Key, entry.Value);
        }
        sw.WriteLine("===========================================");


        end_metric:

        sw.WriteLine("=================    THE END   ================");
        sw.Flush();
        sw.Close();

        logger.LogInformation("[MetricGather] Finished collecting metrics.");

    }

    protected abstract Dictionary<TransactionType, int> CollectAborts(DateTime finishTime);

    protected abstract List<Latency> CollectFromSeller(DateTime finishTime);

    protected abstract List<Latency> CollectFromCustomer(DateTime finishTime);

    protected abstract List<Latency> CollectFromDelivery(DateTime finishTime);
}

