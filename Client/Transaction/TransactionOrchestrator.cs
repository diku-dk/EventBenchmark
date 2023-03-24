using Orleans;
using GrainInterfaces.Scenario;
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
        private readonly Random random = new Random();

        // orleans client
        private readonly IClusterClient orleansClient;

        // streams
        private readonly IStreamProvider streamProvider;

        private readonly ScenarioConfiguration scenarioConfiguration;

        //private readonly Dictionary<WorkloadType, long> nextIdPerTxType = new Dictionary<WorkloadType, long>();

        public TransactionOrchestrator(IClusterClient clusterClient, ScenarioConfiguration scenarioConfiguration) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            this.scenarioConfiguration = scenarioConfiguration;
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

            switch (scenarioConfiguration.submissionStrategy)
            {
                case SubmissionStrategy.BURST_THEN_CONTROL:
                {
                    List<Task> tasks = new();
                    if (scenarioConfiguration.submissionType == SubmissionEnum.TIME_IN_MILLI)
                    {
                        int milli = DateTime.Now.Millisecond;
                        int stopAt = milli + scenarioConfiguration.windowOrBurstValue;

                        do {
                            tasks.Add( Task.Run(() => SubmitTransaction()) );
                        } while (DateTime.Now.Millisecond < stopAt);

                    }
                    else
                    {
                        int val = scenarioConfiguration.windowOrBurstValue;
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
                        if(scenarioConfiguration.waitBetweenSubmissions > 0)
                        {
                            await Task.Delay(TimeSpan.FromMilliseconds(scenarioConfiguration.waitBetweenSubmissions));
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
            int idx = random.Next(0, this.scenarioConfiguration.weight.Length);
            WorkloadType tx = this.scenarioConfiguration.weight[idx];

            long grainID = scenarioConfiguration.numGenPerTxType[tx].NextValue();

            switch (tx)
            { 
                case WorkloadType.CUSTOMER_SESSION: //customer
                {
                    // pick a random customer ID
                    // but make sure there is no active session for the customer. if so, pick another customer
                    if (customerStatusCache.ContainsKey(grainID)) {
                        while (customerStatusCache[grainID] == CustomerStatus.BROWSING)
                            grainID = scenarioConfiguration.numGenPerTxType[tx].NextValue();
                    } else {
                        customerStatusCache.GetOrAdd(grainID, CustomerStatus.NEW);
                    }
                    
                    ICustomerWorker customerWorker = orleansClient.GetGrain<ICustomerWorker>(grainID);
                    if(customerStatusCache[grainID] == CustomerStatus.NEW)
                        await customerWorker.Init();
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, grainID.ToString());
                    await streamOutgoing.OnNextAsync(1);
                    break;
                }
                case WorkloadType.PRICE_UPDATE: // seller
                {
                    ISellerWorker sellerWorker = orleansClient.GetGrain<ISellerWorker>(grainID);
                    if (!sellerStatusCache.ContainsKey(grainID))
                    {
                        await sellerWorker.Init();
                    }
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    await streamOutgoing.OnNextAsync(0);
                    return;
                }
                case WorkloadType.DELETE_PRODUCT: // seller
                {
                    ISellerWorker sellerWorker = orleansClient.GetGrain<ISellerWorker>(grainID);
                    if (!sellerStatusCache.ContainsKey(grainID))
                    {
                        await sellerWorker.Init();
                    }
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.SellerStreamId, grainID.ToString());
                    await streamOutgoing.OnNextAsync(1);
                    return;
                }
                case WorkloadType.UPDATE_DELIVERY: // delivery worker
                {
                    return;
                }
                default: { throw new Exception("Unknown transaction type defined!"); }
            }

            return;
        }

    }
}

