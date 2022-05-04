using RabbitMQ.Client;
using Orleans;
using System.Collections.Generic;
using Common.Configuration;

namespace Client.RabbitMQ
{

    /**
     * https://www.rabbitmq.com/tutorials/tutorial-one-dotnet.html
     */
    public class RabbitMQConsumerSetup
    {

        private readonly Dictionary<string, QueueToActorEntry> queueToActorMap;
        private readonly ConnectionFactory connectionFactory;
        private readonly IClusterClient ClusterClient;

        public RabbitMQConsumerSetup(string host, int port, List<string> queues, IClusterClient clusterClient, Dictionary<string,QueueToActorEntry> queueToActorMap )
        {
            this.ClusterClient = clusterClient;
            this.connectionFactory = new ConnectionFactory() { HostName = host, Port = port };
            this.queueToActorMap = queueToActorMap;
        }

        /*
         * Each channel consumes a relatively small amount of memory on the client. 
         * Depending on client library's implementation detail it can also use a dedicated 
         * thread pool (or similar) where consumer operations are dispatched, and therefore 
         * one or more threads (or similar).
         */

        public void Init()
        {

            using var connection = connectionFactory.CreateConnection();

            connectionFactory.DispatchConsumersAsync = true;
            connectionFactory.UseBackgroundThreadsForIO = true;

            // TODO number of queues?
            // starts number of loops == concurrency
            // https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/9ccf87a4e5dc997999d614af426db6c6045e5372/projects/RabbitMQ.Client/client/impl/ConsumerDispatching/ConsumerDispatcherChannelBase.cs
            // implements the loop
            // https://github.com/rabbitmq/rabbitmq-dotnet-client/blob/9ccf87a4e5dc997999d614af426db6c6045e5372/projects/RabbitMQ.Client/client/impl/ConsumerDispatching/AsyncConsumerDispatcher.cs
            connectionFactory.ConsumerDispatchConcurrency = 2;


            using var channel = connection.CreateModel();

            // TODO can I create all consumers within the same channel?
            // https://www.rabbitmq.com/channels.html
            // AMQP 0-9-1 connections are multiplexed with channels that can be thought of as "lightweight connections that share a single TCP connection".
            // limiting the number of channels used per connection is highly recommended. As a rule of thumb, most applications can use a single digit number of channels per connection. 

            foreach (KeyValuePair<string, QueueToActorEntry> entry in queueToActorMap)
            {

                var consumer = new CustomConsumer(channel, ClusterClient, entry.Value.actorId);

                _ = channel.QueueDeclare(queue: entry.Key,
                                 durable: false,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

                channel.BasicConsume(queue: entry.Key,
                                     autoAck: true,
                                     consumer: consumer);

            }

        }

    }
}
