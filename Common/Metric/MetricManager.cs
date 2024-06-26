﻿using Common.Entities;
using Common.Infra;
using Common.Services;
using Common.Workload;
using Common.Workload.Metrics;
using Microsoft.Extensions.Logging;

namespace Common.Metric;

public class MetricManager
{

    public delegate MetricManager BuildMetricManagerDelegate(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService);

    protected static readonly ILogger logger = LoggerProxy.GetInstance("MetricManager");

    protected readonly ISellerService sellerService;
    protected readonly ICustomerService customerService;
    protected readonly IDeliveryService deliveryService;

    protected int numSellers;
    protected int numCustomers;

    protected MetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService)
	{
        this.sellerService = sellerService;
        this.customerService = customerService;
        this.deliveryService = deliveryService;
	}

    public static MetricManager BuildMetricManager(ISellerService sellerService, ICustomerService customerService, IDeliveryService deliveryService)
    {
        return new MetricManager(sellerService, customerService, deliveryService);
    }

    public void SetUp(int numSellers, int numCustomers)
    {
        this.numSellers = numSellers;
        this.numCustomers = numCustomers;
    }

    protected static List<Latency> BuildLatencyList(Dictionary<object, TransactionIdentifier> submitted, Dictionary<object, TransactionOutput> finished, DateTime finishTime, string workerType = "")
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

    public static void SimpleCollect(DateTime startTime, DateTime finishTime, int numberTIDs, string runName = null)
    {
        StreamWriter sw = BuildStreamWriter(startTime, finishTime, runName);
        
        TimeSpan executionTime = finishTime - startTime;
        CalculateOverallThroughput(sw, numberTIDs, executionTime);

        CloseStreamWriter(sw);
    }

    public void Collect(DateTime startTime, DateTime finishTime, int epochPeriod = 0, string runName = null)
    {
        logger.LogInformation("Starting collecting metrics for run between {0} and {1}", startTime, finishTime);

        StreamWriter sw = BuildStreamWriter(startTime, finishTime, runName);

        List<TransactionType> txTypeValues = Enum.GetValues(typeof(TransactionType)).Cast<TransactionType>().ToList();
        List<List<Latency>> latencyGatherResults = this.CalculateLatency(finishTime, sw, txTypeValues);

        int countTid = 0;
        foreach (var list in latencyGatherResults)
        {
            countTid += list.Count;
        }

        // transactions per second
        TimeSpan executionTime = finishTime - startTime;
        CalculateOverallThroughput(sw, countTid, executionTime);

        if (epochPeriod <= 0 || epochPeriod >= executionTime.Milliseconds)
        {
            logger.LogWarning("Skipping breakdown calculation since epoch is outside allowed range!");
        } else
        {
            BreakdownCalculation(startTime, finishTime, epochPeriod, sw, latencyGatherResults, txTypeValues, executionTime);
        }

        CalculateAborts(finishTime, sw);

        CalculateReplicationAnomalies(finishTime, sw);

        CloseStreamWriter(sw);

        logger.LogInformation("Finished collecting metrics.");
    }

    private void CalculateReplicationAnomalies(DateTime finishTime, StreamWriter sw)
    {
        // collect anomalies caused by replication
        int anomalies = this.CollectReplicationAnomalies(finishTime);
        if (anomalies > 0)
        {
            logger.LogInformation("================== Anomalies ==================");
            sw.WriteLine("================== Anomalies ==================");
            logger.LogInformation("Number of collected anomalies: {0}", anomalies);
            sw.WriteLine("Number of collected anomalies: {0}", anomalies);
        }
    }

    private void CalculateAborts(DateTime finishTime, StreamWriter sw)
    {
        logger.LogInformation("================== Aborts ==================");
        sw.WriteLine("================== Aborts ==================");
        Dictionary<TransactionType, int> dictCount = this.CollectAborts(finishTime);
        foreach (var entry in dictCount)
        {
            logger.LogInformation("Transaction: {0}: {1}", entry.Key, entry.Value);
            sw.WriteLine("Transaction: {0}: {1}", entry.Key, entry.Value);
        }
        logger.LogInformation("===========================================");
        sw.WriteLine("===========================================");
    }

    private static void CloseStreamWriter(StreamWriter sw)
    {
        logger.LogInformation("=================    THE END   ================");
        sw.WriteLine("=================    THE END   ================");
        sw.Flush();
        sw.Close();
    }

    private static void CalculateOverallThroughput(StreamWriter sw, int countTid, TimeSpan executionTime)
    {
        double txPerSecond = countTid / executionTime.TotalSeconds;

        logger.LogInformation("Number of seconds: {0}", executionTime.TotalSeconds);
        sw.WriteLine("Number of seconds: {0}", executionTime.TotalSeconds);
        logger.LogInformation("Number of completed transactions: {0}", countTid);
        sw.WriteLine("Number of completed transactions: {0}", countTid);
        logger.LogInformation("Transactions per second: {0}", txPerSecond);
        sw.WriteLine("Transactions per second: {0}", txPerSecond);
        sw.WriteLine("=====================================================");
    }

    private static StreamWriter BuildStreamWriter(DateTime startTime, DateTime finishTime, string runName)
    {
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
        return sw;
    }

    private List<List<Latency>> CalculateLatency(DateTime finishTime, StreamWriter sw, List<TransactionType> txTypeValues)
    {
        // seller workers
        List<Latency> sellerLatencyList = this.CollectFromSeller(finishTime);
        // customer workers
        List<Latency> customerLatencyList = this.CollectFromCustomer(finishTime);
        // delivery worker
        List<Latency> deliveryLatencyList = this.CollectFromDelivery(finishTime);

        List<List<Latency>> latencyGatherResults = new List<List<Latency>>
        {
            sellerLatencyList,
            customerLatencyList,
            deliveryLatencyList
        };
        Dictionary<TransactionType, List<double>> latencyCollPerTxType = new();

        foreach (var txType in txTypeValues)
        {
            latencyCollPerTxType.Add(txType, new List<double>());
        }

        foreach (var list in latencyGatherResults)
        {
            foreach (var entry in list)
            {
                latencyCollPerTxType[entry.type].Add(entry.totalMilliseconds);
            }
        }

        foreach (var entry in latencyCollPerTxType)
        {
            double avg = 0;
            if (entry.Value.Count > 0)
            {
                avg = entry.Value.Average();
            }
            logger.LogInformation("Transaction: {0} - #{1} - Average end-to-end latency: {2}", entry.Key, entry.Value.Count, avg.ToString());
            sw.WriteLine("Transaction: {0} - #{1} - Average end-to-end latency: {2}", entry.Key, entry.Value.Count, avg.ToString());
        }

        return latencyGatherResults;
    }

    private static void BreakdownCalculation(DateTime startTime, DateTime finishTime, int epochPeriod, StreamWriter sw, List<List<Latency>> latencyGatherResults, List<TransactionType> txTypeValues, TimeSpan executionTime)
    {
        // TODO calculate percentiles
        // break down latencies by end timestamp
        int numEpochs = (int)executionTime.TotalMilliseconds / epochPeriod;
        logger.LogInformation("{0} blocks for epoch {1}", numEpochs, epochPeriod);
        sw.WriteLine("{0} blocks for epoch {1}", numEpochs, epochPeriod);

        List<Dictionary<TransactionType, List<double>>> epochs = new(numEpochs);
        for (int i = 0; i < numEpochs; i++)
        {
            epochs.Add(new());
            foreach (var txType in txTypeValues)
            {
                epochs[i].Add(txType, new List<double>());
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

                if (idx >= epochs.Count)
                {
                    idx = epochs.Count - 1;
                }

                if (idx >= 0 && entry.endTimestamp <= finishTime)
                {
                    epochs[idx][entry.type].Add(entry.totalMilliseconds);
                }
            }
        }

        int epochIdx = 1;
        foreach (var epoch in epochs)
        {
            logger.LogInformation("Block {0} results:", epochIdx);
            sw.WriteLine("Block {0} results:", epochIdx);

            // iterating over each transaction type in block
            int epochCountTid = 0;
            foreach (var entry in epoch)
            {
                double avg = 0;
                if (entry.Value.Count > 0)
                {
                    avg = entry.Value.Average();
                }
                epochCountTid += entry.Value.Count;
                logger.LogInformation("Transaction: {0} - #{1} - Average end-to-end latency: {2}", entry.Key, entry.Value.Count, avg.ToString());
                sw.WriteLine("Transaction: {0} - #{1} - Average end-to-end latency: {2}", entry.Key, entry.Value.Count, avg.ToString());
            }

            double epochTxPerSecond = epochCountTid / (epochPeriod / 1000d);

            logger.LogInformation("Number of completed transactions: {0}", epochCountTid);
            sw.WriteLine("Number of completed transactions: {0}", epochCountTid);
            logger.LogInformation("Transactions per second: {0}", epochTxPerSecond);
            sw.WriteLine("Transactions per second: {0}", epochTxPerSecond);

            epochIdx++;
            sw.WriteLine("===========================================");
        }
    }

    protected virtual Dictionary<TransactionType, int> CollectAborts(DateTime finishTime)
    {
        Dictionary<TransactionType, int> abortCount = new()
        {
            { TransactionType.PRICE_UPDATE, 0 },
             { TransactionType.UPDATE_PRODUCT, 0 },
              { TransactionType.CUSTOMER_SESSION, 0 },
                { TransactionType.QUERY_DASHBOARD, 0 },
                  { TransactionType.UPDATE_DELIVERY, 0 },
        };
        var sellerAborts = this.sellerService.GetAbortedTransactions();
        foreach(var abort in sellerAborts){
            abortCount[abort.type]++;
        }

        var customerAborts = this.customerService.GetAbortedTransactions();
        abortCount[TransactionType.CUSTOMER_SESSION] += customerAborts.Count;

        var deliveryAborts = deliveryService.GetAbortedTransactions();
        abortCount[TransactionType.UPDATE_DELIVERY] += deliveryAborts.Count;
        
        return abortCount;
    }

    protected virtual List<Latency> CollectFromSeller(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> sellerSubmitted = new();
        Dictionary<object, TransactionOutput> sellerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= numSellers; i++)
        {
            var submitted = this.sellerService.GetSubmittedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!sellerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    logger.LogDebug("[Seller] Duplicate submitted transaction entry found. Existing {0} New {1} ", sellerSubmitted[tx.tid], tx);
                }
            }

            var finished = this.sellerService.GetFinishedTransactions(i);
            foreach (var tx in finished)
            {
                if (!sellerFinished.TryAdd(tx.tid, tx))
                {
                    dupFin++;
                    logger.LogDebug("[Seller] Duplicate finished transaction entry found. Existing {0} New {1} ", sellerFinished[tx.tid], finished);
                }
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Seller] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Seller] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(sellerSubmitted, sellerFinished, finishTime, "seller");
    }

    protected virtual List<Latency> CollectFromCustomer(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> customerSubmitted = new();
        Dictionary<object, TransactionOutput> customerFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        for (int i = 1; i <= this.numCustomers; i++)
        {
            var submitted = this.customerService.GetSubmittedTransactions(i);
            var finished = this.customerService.GetFinishedTransactions(i);
            foreach (var tx in submitted)
            {
                if (!customerSubmitted.TryAdd(tx.tid, tx))
                {
                    dupSub++;
                    logger.LogDebug("[Customer] Duplicate submitted transaction entry found. Existing {0} New {1} ", customerSubmitted[tx.tid], tx);
                }
            }

            foreach (var tx in finished)
            {
                if (!customerFinished.TryAdd(tx.tid, tx))
                {
                    dupFin++;
                    logger.LogDebug("[Customer] Duplicate finished transaction entry found. Existing {0} New {1} ", customerFinished[tx.tid], tx);
                }
            }

        }

        if (dupSub > 0)
            logger.LogWarning("[Customer] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Customer] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(customerSubmitted, customerFinished, finishTime, "customer");
    }

    public virtual List<Latency> CollectFromDelivery(DateTime finishTime)
    {
        Dictionary<object, TransactionIdentifier> deliverySubmitted = new();
        Dictionary<object, TransactionOutput> deliveryFinished = new();

        int dupSub = 0;
        int dupFin = 0;

        var submitted = this.deliveryService.GetSubmittedTransactions();
        foreach (var tx in submitted)
        {
            if (!deliverySubmitted.TryAdd(tx.tid, tx))
            {
                dupSub++;
                logger.LogDebug("[Delivery] Duplicate submitted transaction entry found. Existing {0} New {1} ", deliverySubmitted[tx.tid], tx);
            }
        }

        var finished = this.deliveryService.GetFinishedTransactions();
        foreach (var tx in finished)
        {
            if (!deliveryFinished.TryAdd(tx.tid, tx))
            {
                dupFin++;
                logger.LogDebug("[Delivery] Duplicate finished transaction entry found. Existing {0} New {1} ", deliveryFinished[tx.tid], tx);
            }
        }

        if (dupSub > 0)
            logger.LogWarning("[Delivery] Number of duplicated submitted transactions found: {0}", dupSub);
        if (dupFin > 0)
            logger.LogWarning("[Delivery] Number of duplicated finished transactions found: {0}", dupFin);

        return BuildLatencyList(deliverySubmitted, deliveryFinished, finishTime, "delivery");
    }

    protected int CollectReplicationAnomalies(DateTime finishTime)
    {
        IDictionary<int, List<Product>> productUpdatesPerSeller = this.sellerService.GetTrackedProductUpdates();
        IDictionary<int, IDictionary<string,List<CartItem>>> cartHistoryPerCustomer = this.customerService.GetCartHistoryPerCustomer(finishTime);
        return DoCollectReplicationAnomalies(productUpdatesPerSeller, cartHistoryPerCustomer);
    }

    /**
     * Public to allow for testing
     */
    public static int DoCollectReplicationAnomalies(IDictionary<int, List<Product>> productUpdatesPerSeller, IDictionary<int, IDictionary<string,List<CartItem>>> cartHistoryPerCustomer)
    {
        int count = 0;
        // verify carts that break causality
        // what breaks causality?
        // a. a version not "seen"
        // b. a price update that should have been applied
        // if there is no other item from the same seller in the cart, we cannot spot the anomaly
        // but it is fine, since we are only looking for causality anomalies
        foreach(int customerId in cartHistoryPerCustomer.Keys)
        {
            var cartItemsPerTid = cartHistoryPerCustomer[customerId];

            foreach(var tidEntry in cartItemsPerTid)
            {
                // group per seller
                var cartItemsBySeller = tidEntry.Value.GroupBy(a=>a.SellerId).ToDictionary(g=>g.Key, g=>g.ToList());
                foreach(var sellerGroup in cartItemsBySeller)
                {
                    ISet<(int,int)> sellerTrack = new HashSet<(int,int)>();

                    // no causality is broken for a single item
                    if(sellerGroup.Value.Count == 1) continue;

                    // price that has not applied but another product shows a version that occurs after that price update
                    List<Product> sellerUpdates = productUpdatesPerSeller[sellerGroup.Key];

                    // we verify causality anomaly by fixing a pivot
                    // up to this pivot, have the cart broken causality?
                    for(int i = 1; i < sellerGroup.Value.Count; i++)
                    {
                        // 1 - find entry in product updates per seller
                        int indexPivot = sellerUpdates.FindIndex(a =>
                                a.product_id == sellerGroup.Value[i].ProductId
                                && a.version.SequenceEqual(sellerGroup.Value[i].Version)
                                && a.price == sellerGroup.Value[i].UnitPrice);

                        // 2 - for each precedent cart item with ID x, walk back in the product updates list trying to find the last update to X and compare

                        // either an update is not applied or was applied in the wrong order
                        // x1 x2 y1. if cart contains y1, then cart must have contain x2
                        for(int j = 0; j < i; j++)
                        {
                            int pos = indexPivot + 1;
                            while(pos < sellerUpdates.Count - 1)
                            {
                                // find the last feasible update
                                if(sellerUpdates[pos].product_id == sellerGroup.Value[j].ProductId
                                    && sellerUpdates[pos].version == sellerGroup.Value[j].Version)
                                {
                                    if(sellerUpdates[pos].price != sellerGroup.Value[j].UnitPrice)
                                    {
                                        // add causality anomaly
                                        sellerTrack.Add((j,pos));
                                    }
                                    else {
                                        // no more anomalies
                                        break;
                                    }
                                }
                                pos++;
                            }
                        }

                        count += sellerTrack.Count;
                    }
                }
            }
        }

        return count;
    }

}

