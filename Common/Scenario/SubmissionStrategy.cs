namespace Common.Scenario
{
    public enum SubmissionStrategy
    {

        // send a burst of transactions and then for every termination a new one is created
        BURST_THEN_CONTROL,
        // transactions are sent at every window
        WINDOW,
        // keeps sending without stopping
        ALL_AT_ONCE


    }
}
