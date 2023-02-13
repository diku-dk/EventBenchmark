using System;

namespace Common.Scenario
{
    public class ScenarioConfiguration
    {

        public readonly SubmissionStrategy submissionStrategy = SubmissionStrategy.BURST_THEN_CONTROL;

        // a timer is configured to notify the orchestrator grain about the termination
        public readonly TimeSpan timeSpan = TimeSpan.FromSeconds(60);

        // e.g. 10 entries, new order has 7 entries and price update 3, meaning 70% probability of new order
        public readonly TransactionType[] weight;

        // how much time a window or burst may remain
        public readonly int windowOrBurstTime = 10000;

    }
}
