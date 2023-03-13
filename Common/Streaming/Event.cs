using System;
namespace Common.Streaming
{
	public class Event
	{
		public string topic { get; }
		// public string key { get; }
		public string payload { get; }

		public Event(string topic, string payload)
		{
			this.topic = topic;
			this.payload = payload;
		}
	}
}

