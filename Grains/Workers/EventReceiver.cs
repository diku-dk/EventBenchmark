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
     * https://dotnet.github.io/orleans/docs/grains/stateless_worker_grains.html
     */
    [StatelessWorker]
    public class EventReceiver : Grain, IEventProcessor
    {

        private IAsyncStream<string> stream;

        public EventReceiver()
        {
            
        }


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
