using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Client.Infra;
using Common.Infra;
using Common.Streaming;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Streams;
using static Confluent.Kafka.ConfigPropertyNames;

namespace Client.Streaming.Kafka
{
	public class KafkaConsumer : Stoppable
	{

        public const int defaultDelay = 5000;
        public static TimeSpan defaultBlockForConsume = TimeSpan.FromSeconds(2.5);
        private readonly Guid streamId;
        private readonly IConsumer<string, Event> consumer;
        private readonly IStreamProvider streamProvider;
		private readonly string kafkaTopic;

        private readonly Dictionary<string, IAsyncStream<Event>> streamCache;

		public KafkaConsumer(IConsumer<string, Event> consumer, IStreamProvider streamProvider, Guid streamId, string kafkaTopic)
		{
			this.consumer = consumer;
            this.streamProvider = streamProvider;
			this.kafkaTopic = kafkaTopic;
            this.streamId = streamId;
            this.streamCache = new Dictionary<string, IAsyncStream<Event>>();
        }

		public async Task Run()
		{
            this.consumer.Subscribe(kafkaTopic);
            ConsumeResult<string, Event> consumeResult;
            while (this.IsRunning())
			{
                // https://github.com/confluentinc/confluent-kafka-dotnet/issues/487
                consumeResult = consumer.Consume(defaultBlockForConsume);
                if(consumeResult == null || consumeResult.Message == null)
                {
                    await Task.Delay(defaultDelay);
                } else
                {
                    if (streamCache.ContainsKey(consumeResult.Message.Key))
                    {
                        await streamCache[consumeResult.Message.Key].OnNextAsync(consumeResult.Message.Value);
                    } else
                    {
                        IAsyncStream<Event> stream = this.streamProvider.GetStream<Event>(streamId, consumeResult.Message.Key);
                        streamCache[consumeResult.Message.Key] = stream;
                        await stream.OnNextAsync(consumeResult.Message.Value);
                    }
                    
                }
            }

		}

        public override void Dispose()
        {
			// https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Web/RequestTimeConsumer.cs
            this.consumer.Close();
            this.consumer.Dispose();
            base.Dispose();
        }

    }
}

