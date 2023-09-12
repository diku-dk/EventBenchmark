using Common.Streaming;

namespace Common.Cleaning
{
	public class CleaningConfig
	{
        public StreamingConfig streamingConfig { get; set; }

        public IDictionary<string, string> mapMicroserviceToUrl { get; set; }

        public const string cleanupEndpoint = "/cleanup";

    }
}

