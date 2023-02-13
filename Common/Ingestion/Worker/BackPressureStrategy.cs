namespace Common.Ingestion.Worker
{
    /**
     * Inspired by https://medium.com/@jayphelps/backpressure-explained-the-flow-of-data-through-software-2350b3e77ce7
     * More info: https://www.baeldung.com/spring-webflux-backpressure
     */
    public enum BackPressureStrategy
    {
        NONE,
        CONTROL,
        BUFFER
        // DROP ---> we cannot drop! =) only the http server

    }
}
