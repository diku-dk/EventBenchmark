﻿using System;
using RabbitMQ.Client;
using Orleans;
using Orleans.Concurrency;
using RabbitMQ.Client.Events;
using System.Text;
using System.Threading.Tasks;
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


        private void InitOriginal()
        {
            string queue = "testQueue";

            using (var connection = connectionFactory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                var consumer = new EventingBasicConsumer(channel);

                // AsyncEventBasicConsumer

                channel.BasicConsume(consumer, queue);

                _ = channel.QueueDeclare(queue: queue,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);


                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };

                channel.BasicConsume(queue: "hello",
                                     autoAck: true,
                                     consumer: consumer);
            }

        }


        //public void InitOriginal1()
        //{

        //    using var connection = connectionFactory.CreateConnection();
        //    using var channel = connection.CreateModel();

        //    int i = 0;
        //    foreach (string queue in queues)
        //    {

        //        var consumer = new EventingBasicConsumer(channel);

        //        channel.BasicConsume(consumer, queue);

        //        _ = channel.QueueDeclare(queue: queue,
        //                         durable: true,
        //                         exclusive: false,
        //                         autoDelete: false,
        //                         arguments: null);


        //        consumer.Received += (model, ea) =>
        //        {
        //            var body = ea.Body.ToArray();
        //            var message = Encoding.UTF8.GetString(body);
        //            Console.WriteLine(" [x] Received {0}", message);
        //            receiver.ReceiveEvent(queues[i], message); // TODO test whether this works fine
        //        };

        //        channel.BasicConsume(queue: queue,
        //                             autoAck: true,
        //                             consumer: consumer);

        //        i++;
        //    }


        //}



    }
}
