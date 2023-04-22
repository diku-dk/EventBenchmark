using Orleans;
using System.Threading.Tasks;
using Common.Scenario;
using System;
using System.Collections.Generic;
using GrainInterfaces.Workers;
using Common.Streaming;
using Orleans.Streams;
using System.Collections.Concurrent;
using Common.Scenario.Customer;
using Common.Infra;
using Common.Configuration;
using Common.YCSB;
using Microsoft.Extensions.Logging;
using Client.Infra;

namespace Transaction
{

    /*
     * Aka transaction submission 
     * sets up the service grain. for each external service, properly set the event listener
     *  https://www.google.com/search?client=firefox-b-d&q=grain+as+socket+server+orleans
     *  https://stackoverflow.com/questions/55021791/orleans-custom-tcp-socket-connection
     * 
     */
    public class TransactionOrchestrator : Stoppable
    {
        private readonly Random random;

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ScenarioConfiguration config;

        // customer and seller workers do not need to know about other customer
        // but the transaction orchestrator needs to assign grain ids to the transactions emitted
        // public readonly Distribution customerDistribution;
        // public readonly Range customerRange;

        // provides an ID generator for each workload (e.g., customer, seller)
        // the generator obeys a distribution
        public readonly Dictionary<WorkloadType, NumberGenerator> keyGeneratorPerWorkloadType;

        private StreamSubscriptionHandle<CustomerStatusUpdate> customerWorkerSubscription;

        private readonly Dictionary<long, int> sellerStatusCache;

        private readonly ConcurrentDictionary<long, CustomerStatus> customerStatusCache;

        private readonly ILogger _logger;

        public TransactionOrchestrator(IClusterClient clusterClient, ScenarioConfiguration scenarioConfiguration, Range customerRange) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.config = scenarioConfiguration;
            this.random = new Random();
            
            NumberGenerator sellerIdGenerator = this.config.customerConfig.sellerDistribution == Distribution.UNIFORM ?
                            new UniformLongGenerator(this.config.customerConfig.sellerRange.Start.Value, this.config.customerConfig.sellerRange.End.Value) :
                            new ZipfianGenerator(this.config.customerConfig.sellerRange.Start.Value, this.config.customerConfig.sellerRange.End.Value);

            NumberGenerator customerIdGenerator = this.config.customerDistribution == Distribution.UNIFORM ?
                                    new UniformLongGenerator(customerRange.Start.Value, customerRange.End.Value) :
                                    new ZipfianGenerator(customerRange.Start.Value, customerRange.End.Value);
            this.keyGeneratorPerWorkloadType = new()
            {
                [WorkloadType.PRICE_UPDATE] = sellerIdGenerator,
                [WorkloadType.DELETE_PRODUCT] = sellerIdGenerator,
                [WorkloadType.CUSTOMER_SESSION] = customerIdGenerator
            };

            this.sellerStatusCache = new();
            this.customerStatusCache = new();

            this._logger = LoggerProxy.GetInstance();
        }

        private async void ConfigureStream()
        {
            IAsyncStream<CustomerStatusUpdate> resultStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfiguration.CustomerStreamId, StreamingConfiguration.TransactionStreamNameSpace);
            this.customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        public async Task Run()
        {
            ConfigureStream();

            Console.WriteLine("Transaction orchestrator execution started.");

            switch (config.submissionStrategy)
            {
                case SubmissionStrategy.BURST_THEN_CONTROL:
                {
                    List<Task> tasks = new();
                    if (config.submissionType == SubmissionEnum.TIME_IN_MILLI)
                    {
                        int milli = DateTime.Now.Millisecond;
                        int stopAt = milli + config.submissionValue;

                        do {
                            tasks.Add( Task.Run(SubmitTransaction) );
                        } while (DateTime.Now.Millisecond < stopAt);

                    }
                    else
                    {
                        int val = config.submissionValue;
                        do {
                            tasks.Add( Task.Run(SubmitTransaction) );
                            val--;
                        } while (val > 0);
                    }

                    Console.WriteLine("Scenario orchestrator first batch terminated! Initializing main loop.");

                    while (this.IsRunning())
                    {
                        Task completedTask = await Task.WhenAny(tasks);
                        tasks.Remove(completedTask);
                        if(config.waitBetweenSubmissions > 0)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(config.waitBetweenSubmissions));
                        }

                        tasks.Add(Task.Run(SubmitTransaction));
                    }
                    
                    break;
                }
                case SubmissionStrategy.CONTINUOUS: 
                {
                    // not supported yet . with pause between submisions or not
                    break;
                }
                case SubmissionStrategy.WINDOW:
                {
                    // not supported yet
                    break;
                }
                default: { throw new Exception("Strategy for submitting transactions not defined!"); }
            }

            Console.WriteLine("Scenario orchestrator main loop terminated.");

            await customerWorkerSubscription.UnsubscribeAsync();

            return;
        }

        private Task UpdateCustomerStatusAsync(CustomerStatusUpdate update, StreamSequenceToken token = null)
        {
            this._logger.LogWarning("Changed customer status of customer {0} in cache. Previous {1} Update {2}",
                update.customerId, this.customerStatusCache[update.customerId], update.status);
            this.customerStatusCache[update.customerId] = update.status;
            return Task.CompletedTask;
        }

        /**
         * Synthetic. real data set may be different...
         * 
         */
        private async Task SubmitTransaction()
        {
            try
            {
                this._logger.LogWarning("Submit transaction called!");

                int idx = random.Next(0, this.config.weight.Length);
                this._logger.LogWarning("Index:{0}", idx);

                WorkloadType tx = this.config.weight[idx];

                this._logger.LogWarning("Transaction type {0}", tx.ToString());

                long grainID;

                switch (tx)
                {
                    //customer
                    case WorkloadType.CUSTOMER_SESSION:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                        // but make sure there is no active session for the customer. if so, pick another customer
                        if (this.customerStatusCache.ContainsKey(grainID))
                        {
                            while (this.customerStatusCache.ContainsKey(grainID) &&
                                    customerStatusCache[grainID] == CustomerStatus.BROWSING)
                            {
                                grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                            }
                        }
                        else
                        {
                            this.customerStatusCache.TryAdd(grainID, CustomerStatus.NEW);
                        }

                        this._logger.LogWarning("Customer worker {0} defined!", grainID);

                        ICustomerWorker customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(grainID);
                        if (this.customerStatusCache[grainID] == CustomerStatus.NEW)
                        {
                            await customerWorker.Init(config.customerConfig);
                        }
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, grainID.ToString());
                        this.customerStatusCache[grainID] = CustomerStatus.BROWSING;
                        _ = streamOutgoing.OnNextAsync(1);
                        this._logger.LogWarning("Customer worker {0} message sent!", grainID);
                        break;
                    }
                    // seller
                    case WorkloadType.PRICE_UPDATE:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(0);
                        return;
                    }
                    // seller
                    case WorkloadType.DELETE_PRODUCT:
                    {
                        grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                        _ = streamOutgoing.OnNextAsync(1);
                        return;
                    }
                    // delivery worker
                    case WorkloadType.UPDATE_DELIVERY:
                    {
                        var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.DeliveryStreamId, null);
                        _ = streamOutgoing.OnNextAsync(0);
                        return;
                    }
                    default: { throw new Exception("Unknown transaction type defined!"); }
                }
            } catch (Exception e)
            {
                this._logger.LogError("Error caught in SubmitTransaction: {0}", e.Message);
            }

            return;
        }

    }
}

