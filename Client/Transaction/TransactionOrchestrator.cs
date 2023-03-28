using Orleans;
using System.Threading.Tasks;
using Common.Scenario;
using System;
using System.Collections.Generic;
using GrainInterfaces.Workers;
using Common.Streaming;
using System.Text;
using Orleans.Streams;
using System.Net.NetworkInformation;
using Client.Infra;
using System.Collections.Concurrent;
using Common.Scenario.Customer;
using Common.Infra;

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

        public TransactionOrchestrator(IClusterClient clusterClient, ScenarioConfiguration scenarioConfiguration) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.config = scenarioConfiguration;
            this.random = new Random();
        }

        private StreamSubscriptionHandle<CustomerStatusUpdate> customerSubscription;

        private async void ConfigureStream()
        {
            IAsyncStream<CustomerStatusUpdate> resultStream = streamProvider.GetStream<CustomerStatusUpdate>(StreamingConfiguration.CustomerStreamId, StreamingConfiguration.TransactionStreamNameSpace);
            customerSubscription = await resultStream.SubscribeAsync(UpdateCustomerStatusAsync);
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
                            tasks.Add( Task.Run(() => SubmitTransaction()) );
                        } while (DateTime.Now.Millisecond < stopAt);

                    }
                    else
                    {
                        int val = config.submissionValue;
                        do {
                            tasks.Add(Task.Run(SubmitTransaction));
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

                        tasks.Add(Task.Run(() => SubmitTransaction()));
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
                default: { throw new Exception(); }
            }

            Console.WriteLine("Scenario orchestrator main loop terminated.");

            await customerSubscription.UnsubscribeAsync();

            return;
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
            int idx = random.Next(0, this.config.weight.Length);
            WorkloadType tx = this.config.weight[idx];

            long grainID = config.keyGeneratorPerWorkloadType[tx].NextValue();

            switch (tx)
            { 
                case WorkloadType.CUSTOMER_SESSION: //customer
                {
                    // but make sure there is no active session for the customer. if so, pick another customer
                    if (customerStatusCache.ContainsKey(grainID)) {
                        while (customerStatusCache[grainID] == CustomerStatus.BROWSING)
                            grainID = config.keyGeneratorPerWorkloadType[tx].NextValue();
                    } else {
                        customerStatusCache.GetOrAdd(grainID, CustomerStatus.NEW);
                    }
                    
                    ICustomerWorker customerWorker = orleansClient.GetGrain<ICustomerWorker>(grainID);
                    if(customerStatusCache[grainID] == CustomerStatus.NEW)
                        await customerWorker.Init(config.customerConfig);
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, grainID.ToString());
                    customerStatusCache[grainID] = CustomerStatus.BROWSING;
                    _ = streamOutgoing.OnNextAsync(1);   
                    break;
                }
                // to make sure the key distribution of sellers folow of the customers
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
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    _ = streamOutgoing.OnNextAsync(0);
                    return;
                }
                case WorkloadType.DELETE_PRODUCT: // seller
                {
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    _ = streamOutgoing.OnNextAsync(1);
                    return;
                }
                case WorkloadType.UPDATE_DELIVERY: // delivery worker
                {
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.DeliveryStreamId, null);
                    _ = streamOutgoing.OnNextAsync(1);
                    return;
                }
                default: { throw new Exception("Unknown transaction type defined!"); }
            }

            return;
        }

    }
}

