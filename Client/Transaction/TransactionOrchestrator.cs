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

        // provides an ID generator for each workload (e.g., customer, seller)
        // the generator obeys a distribution
        public Dictionary<WorkloadType, NumberGenerator> keyGeneratorPerWorkloadType;

        private StreamSubscriptionHandle<CustomerStatusUpdate> customerWorkerSubscription;

        public TransactionOrchestrator(IClusterClient clusterClient, ScenarioConfiguration scenarioConfiguration) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.config = scenarioConfiguration;
            this.random = new Random();
            this.keyGeneratorPerWorkloadType = new(2);
        }

        private async void ConfigureStream()
        {
            IAsyncStream<CustomerStatusUpdate> resultStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfiguration.CustomerStreamId, StreamingConfiguration.TransactionStreamNameSpace);
            customerWorkerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
        }

        public async Task Run()
        {

            ConfigureStream();

            ConfigureDistribution();

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

        private void ConfigureDistribution()
        {
            NumberGenerator sellerIdGenerator = this.config.customerConfig.sellerDistribution == Distribution.UNIFORM ?
                                    new UniformLongGenerator(this.config.customerConfig.sellerRange.Start.Value, this.config.customerConfig.sellerRange.End.Value) :
                                    new ZipfianGenerator(this.config.customerConfig.sellerRange.Start.Value, this.config.customerConfig.sellerRange.End.Value);

            NumberGenerator customerIdGenerator = this.config.customerDistribution == Distribution.UNIFORM ?
                                    new UniformLongGenerator(this.config.customerRange.Start.Value, this.config.customerRange.End.Value) :
                                    new ZipfianGenerator(this.config.customerRange.Start.Value, this.config.customerRange.End.Value);

            this.keyGeneratorPerWorkloadType[WorkloadType.PRICE_UPDATE] = sellerIdGenerator;
            this.keyGeneratorPerWorkloadType[WorkloadType.DELETE_PRODUCT] = sellerIdGenerator;
            this.keyGeneratorPerWorkloadType[WorkloadType.CUSTOMER_SESSION] = customerIdGenerator;
            // this.keyGeneratorPerWorkloadType[WorkloadType.UPDATE_DELIVERY] = sellerIdGenerator;
        }

        private ConcurrentDictionary<long, CustomerStatus> customerStatusCache = new();

        private Task UpdateCustomerStatusAsync(CustomerStatusUpdate update, StreamSequenceToken token = null)
        {
            customerStatusCache[update.customerId] = update.status;
            return Task.CompletedTask;
        }

        private Dictionary<long, int> sellerStatusCache = new();

        /**
         * Synthetic. real data set may be different...
         * 
         */
        private async Task SubmitTransaction()
        {
            Console.WriteLine("Submit transaction called!");

            int idx = random.Next(0, this.config.weight.Length);
            Console.WriteLine("index:{0}", idx);

            WorkloadType tx = this.config.weight[idx];

            Console.WriteLine("Transaction type {0}", tx.ToString());

            long grainID;     

            switch (tx)
            { 
                case WorkloadType.CUSTOMER_SESSION: //customer
                {
                    grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                    // but make sure there is no active session for the customer. if so, pick another customer
                    if (this.customerStatusCache.ContainsKey(grainID)) {
                        while (customerStatusCache[grainID] == CustomerStatus.BROWSING)
                            grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                    } else {
                        this.customerStatusCache.GetOrAdd(grainID, CustomerStatus.NEW);
                    }
                    
                    ICustomerWorker customerWorker = this.orleansClient.GetGrain<ICustomerWorker>(grainID);
                    if (this.customerStatusCache[grainID] == CustomerStatus.NEW)
                    {
                        await customerWorker.Init(config.customerConfig);
                    }
                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, grainID.ToString());
                    this.customerStatusCache[grainID] = CustomerStatus.BROWSING;
                    _ = streamOutgoing.OnNextAsync(1);   
                    break;
                }
                // to make sure the key distribution of sellers follow of the customers
                // the triggering of seller workers must also be based on the customer key distribution
                // an option is having a seller proxy that will match the product to the seller grain call...

                // Q0. what id a relistic behavior for customers?
                // what distribution should be followed? we have many products, categories.
                // instead of key distribution, seller distribution.. and then pick the products. could pick the same seller again
                // Q1. the operations the seller are doing must also obey the key distribution of customers?

                // consumer demand model... the more items bought from a seller, more likely he will increase price
                // we care about stressing the system, less or more conflict
                // we could also have a skewed distribution of products for each seller
                case WorkloadType.PRICE_UPDATE: // seller
                {
                    grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    _ = streamOutgoing.OnNextAsync(0);
                    return;
                }
                case WorkloadType.DELETE_PRODUCT: // seller
                {
                    grainID = this.keyGeneratorPerWorkloadType[tx].NextValue();
                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    _ = streamOutgoing.OnNextAsync(1);
                    return;
                }
                case WorkloadType.UPDATE_DELIVERY: // delivery worker
                {
                    var streamOutgoing = this.streamProvider.GetStream<int>(StreamingConfiguration.DeliveryStreamId, null);
                    _ = streamOutgoing.OnNextAsync(0);
                    return;
                }
                default: { throw new Exception("Unknown transaction type defined!"); }
            }

            return;
        }

    }
}

