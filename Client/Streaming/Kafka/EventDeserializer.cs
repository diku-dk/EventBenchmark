using System;
using System.IO;
using System.Text.Json;
using Common.Streaming;
using Confluent.Kafka;
using Newtonsoft.Json;

namespace Client.Streaming.Kafka
{
	public class EventDeserializer : IDeserializer<string>
	{
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext _)
        {
            if (isNull) return null;
            byte[] bytes = data.ToArray();
            return System.Text.Encoding.UTF8.GetString(bytes, 0, bytes.Length);
        }
    }
}

