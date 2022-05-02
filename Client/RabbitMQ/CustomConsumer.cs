using System;
using System.Text;
using System.Threading.Tasks;
using GrainInterfaces.Workers;
using Orleans;
using RabbitMQ.Client;

namespace Client.RabbitMQ
{
    public class CustomConsumer : AsyncDefaultBasicConsumer
    {

        private readonly IEventReceiver eventReceiver;

        public CustomConsumer(IModel model, IClusterClient client, int actorId) : base(model)
        {

            // client.GetGrain<IPlayerGrain>(receiverActor);
            this.eventReceiver = client.GetGrain<IEventReceiver>(actorId);

        }


        public override async Task HandleBasicDeliver(string consumerTag, ulong deliveryTag, bool redelivered, string exchange, string routingKey, IBasicProperties properties, ReadOnlyMemory<byte> body)
        {
            
            //base.HandleBasicDeliver(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body);

            var body_ = body.ToArray();
            var message = Encoding.UTF8.GetString(body_);
            // Console.WriteLine(" [x] Received {0}", message);

            await eventReceiver.ReceiveEvent(message);

            return;


        }



    }
}
