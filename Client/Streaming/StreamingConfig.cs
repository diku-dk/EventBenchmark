using System;
namespace Client.Streaming
{
	public class StreamingConfig
	{
		public string type { get; set; }

		public string host { get; set; }

		public int port { get; set; }

		// all streams to "clean up"
		public string[] streams { get; set; }

		// these events mark the end of a transaction
        // public string[] txStreams { get; set; }
    }
}

