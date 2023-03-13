using System;
using System.Text;
using Common.Streaming;
using Confluent.Kafka;

namespace Client.Streaming.Kafka
{
    public class EventSerializer : ISerializer<string>
    {
        public byte[] Serialize(string data, SerializationContext _)
        {
            return Encoding.UTF8.GetBytes(data);
        }
    }
}

