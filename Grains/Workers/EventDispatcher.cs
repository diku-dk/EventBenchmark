using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Configuration;
using GrainInterfaces.Workers;
using Orleans;
using Orleans.Concurrency;
using Orleans.Streams;

namespace Grains.Workers
{

    /**
     * Responsible for dispatching events to actors.
     * It must be a stateful grain. Balance the number of this actor with the topics to consume.
     * Something like 3 topics per grain.
     */
    public class EventDispatcher : Grain, IEventDispatcher
    {

        private IAsyncStream<string> stream;

   

        public async override Task OnActivateAsync()
        {

            

            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);

            // TODO get all streams
            Dictionary<int, QueueToStreamEntry> queueToStreamsMap;

            var streamProvider = GetStreamProvider("SMSProvider");
            this.stream = streamProvider.GetStream<string>(Constants.playerUpdatesStreamId, Constants.streamNamespace);
            return;

        }

        public async Task ReceiveEvent(string value)
        {
            // map


            // publish
            _ = this.stream.OnNextAsync(value);

            return;

        }
    }
}
