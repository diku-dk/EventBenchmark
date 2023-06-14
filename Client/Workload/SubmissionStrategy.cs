namespace Common.Workload
{
    /**
     * Backpressure strategies
     * Info: https://medium.com/@jayphelps/backpressure-explained-the-flow-of-data-through-software-2350b3e77ce7
     * More info: https://www.baeldung.com/spring-webflux-backpressure
     */
    public enum SubmissionStrategy
    {

        // send a burst of transactions and then for every transaction result, a new one is created
        // basically the CONTROL strategy
        BURST_THEN_CONTROL,
        // transactions are sent at every window
        // basically the BUFFER strategy
        // may miss some transaction requests because the consumer wil not cope with the rate of input
        WINDOW,
        // keeps sending without stopping
        // don't care about consumer
        // basically increase significantly the risk of losing transaction requests
        CONTINUOUS


    }
}
