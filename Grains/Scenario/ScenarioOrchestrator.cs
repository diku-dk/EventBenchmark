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

namespace Grains.Scenario
{

    /*
     * AKA transaction submission 
     * sets up the service grain. for each external service, properly set the event listener
     *  https://www.google.com/search?client=firefox-b-d&q=grain+as+socket+server+orleans
     *  https://stackoverflow.com/questions/55021791/orleans-custom-tcp-socket-connection
     * 
     */
    public class ScenarioOrchestrator : Grain, IScenarioOrchestrator
    {

        private bool running = true;

        private IDisposable timer;

        private readonly Random random = new Random();

        private long guid;

        private IStreamProvider streamProvider;

        private ScenarioConfiguration scenarioConfiguration;

        // 
        Dictionary<TransactionType, long> nextIdPerTxType = new Dictionary<TransactionType, long>();

        public async override Task OnActivateAsync()
        {
            this.guid = this.GetPrimaryKeyLong();
            this.streamProvider = this.GetStreamProvider(StreamingConfiguration.DefaultStreamProvider);
            var streamIncoming = streamProvider.GetStream<object>(StreamingConfiguration.IngestionStreamId, this.guid.ToString());

            var subscriptionHandles = await streamIncoming.GetAllSubscriptionHandles();
            if (subscriptionHandles.Count > 0)
            {
                foreach (var subscriptionHandle in subscriptionHandles)
                {
                    await subscriptionHandle.ResumeAsync(Run);
                }
            }

            await streamIncoming.SubscribeAsync(Run);

            Console.WriteLine("Scenario orchestrator activated!");
        }

        public Task Init(ScenarioConfiguration scenarioConfiguration)
        {
            foreach (TransactionType tx in Enum.GetValues(typeof(TransactionType)))
            {
                nextIdPerTxType.Add(tx, 0);
            }
            this.scenarioConfiguration = scenarioConfiguration;
            
            return Task.CompletedTask;
        }

        private async Task Run(object obj, StreamSequenceToken token = null)
        {
            // setup timer according to the config passed. the timer defines the end of the experiment
            this.timer = this.RegisterTimer(Stop, null, this.scenarioConfiguration.dueTime, this.scenarioConfiguration.period);

            Console.WriteLine("Scenario orchestrator execution started.");

            switch (scenarioConfiguration.submissionStrategy)
            {
                case SubmissionStrategy.BURST_THEN_CONTROL:
                {
                    
                    Dictionary<int,Task> tasksSubmitted = new Dictionary<int, Task>();

                    if (scenarioConfiguration.submissionType == SubmissionEnum.TIME_IN_MILLI)
                    {
                        int milli = DateTime.Now.Millisecond;
                        int stopAt = milli + scenarioConfiguration.windowOrBurstValue;

                        do {
                            var task = SubmitTransaction(scenarioConfiguration.weight);
                            tasksSubmitted.Add(task.Id, task);
                        } while (DateTime.Now.Millisecond < stopAt);

                    } else
                    {
                        int val = scenarioConfiguration.windowOrBurstValue;
                        do {
                            var task = SubmitTransaction(scenarioConfiguration.weight);
                            tasksSubmitted.Add(task.Id, task);
                            val--;
                        } while (val > 0);
                    }

                    Console.WriteLine("Scenario orchestrator first batch terminated! Initializing main loop.");
                    Task completedTask = await Task.WhenAny(tasksSubmitted.Values);
                    while (this.running)
                    {
                        await Task.Delay(1000);
                        tasksSubmitted.Remove(completedTask.Id);
                        completedTask = await Task.WhenAny(tasksSubmitted.Values);
                    }
                    
                    break;
                }
                case SubmissionStrategy.CONTINUOUS: 
                {
                    // not supported yet
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
         * 
         * 
         */
        private Task SubmitTransaction(TransactionType[] weight)
        {
            int idx = random.Next(0, weight.Length);
            TransactionType tx = weight[idx];

            // get from dictionary
            long val = nextIdPerTxType[tx];
            val++;
            nextIdPerTxType[tx] = val;

            switch (tx)
            { 
                case TransactionType.CHECKOUT: 
                {
                    
                    ICustomerWorker customerWorker = GrainFactory.GetGrain<ICustomerWorker>(val);
                    return customerWorker.Run();
                }
                // stateless workers
                case TransactionType.PRICE_UPDATE:
                {
                    // TODO model as stateless worker
                    // register the config like customer worker, together with a function that updates a price
                    return Task.CompletedTask;
                }
                default: { throw new Exception("Unknown transaction type defined!"); }
            }

            return Task.CompletedTask;
        }

        private Task Stop(object arg)
        {
            Console.WriteLine("Submission of transactions will be terminated.");
            this.running = false;
            // dispose timer
            this.timer.Dispose();
            return Task.CompletedTask;
        }

    }
}

