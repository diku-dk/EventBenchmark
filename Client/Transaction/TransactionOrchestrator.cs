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

        private readonly Dictionary<TransactionType, long> nextIdPerTxType = new Dictionary<TransactionType, long>();

        public TransactionOrchestrator(IClusterClient clusterClient, ScenarioConfiguration scenarioConfiguration) : base()
        {
            this.orleansClient = clusterClient;
            this.streamProvider = orleansClient.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            foreach (TransactionType tx in Enum.GetValues(typeof(TransactionType)))
            {
                nextIdPerTxType.Add(tx, 0);
            }
            this.scenarioConfiguration = scenarioConfiguration;
        }

        public async Task Run()
        {

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
                            tasks.Add(Task.Run(() => SubmitTransaction()));
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
            return;
        }

        /**
         * Synthetic. real data set may be different...
         * 
         */
        private async Task SubmitTransaction()
        {
            int idx = random.Next(0, this.scenarioConfiguration.weight.Length);
            TransactionType tx = this.scenarioConfiguration.weight[idx];

            // get from dictionary
            long val = nextIdPerTxType[tx];
            val++;
            nextIdPerTxType[tx] = val;

            switch (tx)
            { 
                case TransactionType.CHECKOUT:
                {
                    ICustomerWorker customerWorker = orleansClient.GetGrain<ICustomerWorker>(val);
                    await customerWorker.Init();
                    var streamOutgoing = streamProvider.GetStream<int>(StreamingConfiguration.CustomerStreamId, val.ToString());
                    await streamOutgoing.OnNextAsync(1);
                    break;
                }
                // stateless workers
                case TransactionType.PRICE_UPDATE:
                {
                    // TODO model as stateless worker
                    // register the config like customer worker, together with a function that updates a price
                    return; // Task.CompletedTask;
                }
                default: { throw new Exception("Unknown transaction type defined!"); }
            }

            return;
        }

    }
}

