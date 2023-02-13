using Orleans;
using GrainInterfaces.Scenario;
using System.Threading.Tasks;
using Common.Scenario;
using System;
using System.Collections.Generic;
using GrainInterfaces.Workers;

namespace Grains.Scenario
{

    /*
     * sets up the service grain. for each external service, properly set the event listener
     *  https://www.google.com/search?client=firefox-b-d&q=grain+as+socket+server+orleans
     *  https://stackoverflow.com/questions/55021791/orleans-custom-tcp-socket-connection
     * 
     */
    public class ScenarioOrchestrator : Grain, IScenarioOrchestrator
    {

        private bool running = true;

        IDisposable timer;

        Random random = new Random();

        // 
        Dictionary<TransactionType, long> nextIdPerTxType = new Dictionary<TransactionType, long>();

        public async override Task OnActivateAsync()
        {
            foreach(TransactionType tx in Enum.GetValues(typeof(TransactionType)))
            {
                nextIdPerTxType.Add(tx, 0);
            }
            await base.OnActivateAsync();
            return;
        }

        /**
         * Later, to make more agnostic, receive as parameter a config builder
         */
        public async Task Init(ScenarioConfiguration scenarioConfiguration)
        {

            // setup timer according to the config passed. the timer defines the end of the experiment
            this.timer = this.RegisterTimer(Finish, null, TimeSpan.FromSeconds(5), scenarioConfiguration.timeSpan);

            // set customer config


            switch(scenarioConfiguration.submissionStrategy) 
            {
                case SubmissionStrategy.BURST_THEN_CONTROL:
                {
                    int milli = DateTime.Now.Millisecond;
                    int stopAt = milli + scenarioConfiguration.windowOrBurstTime;

                    List<Task> tasksSubmitted = new List<Task>();

                    do {
                        tasksSubmitted.Add(SubmitTransaction(scenarioConfiguration.weight));
                    } while(DateTime.Now.Millisecond >= stopAt);

                    Task completedTask = await Task.WhenAny(tasksSubmitted);
                    while (this.running)
                    {
                        tasksSubmitted.Remove(completedTask);
                        completedTask = await Task.WhenAny(tasksSubmitted);
                    }
                    
                    break;
                }
                case SubmissionStrategy.ALL_AT_ONCE: 
                {
                    break;
                }
                case SubmissionStrategy.WINDOW: { break; }
                default: { throw new Exception(); }
            }

            return;
        }

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
                case TransactionType.NEW_ORDER: 
                {
                    ICustomerWorker customerWorker = GrainFactory.GetGrain<ICustomerWorker>(val);
                    return customerWorker.Run();
                }

            }

            return Task.CompletedTask;
        }

        private Task Finish(object arg)
        {
            this.running = false;
            // dispose timer
            this.timer.Dispose();
            return Task.CompletedTask; 
        }

    }
}

