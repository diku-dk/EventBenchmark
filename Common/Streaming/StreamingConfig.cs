namespace Common.Streaming
{
	public class StreamingConfig
	{
		public string type { get; set; }

		public string host { get; set; }

		public int port { get; set; }

		// all streams to "clean up"
		public string[] streams { get; set; }
    }
}

