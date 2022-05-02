using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Configuration;
using GrainInterfaces.Workers;
using Orleans;

namespace Grains.Workers
{

    /**
     * 
     */

    public class EventReceiver : Grain, IEventReceiver
    {
        public EventReceiver()
        {
            
        }


        public async override Task OnActivateAsync()
        {

            

            IMetadataService metadataService = GrainFactory.GetGrain<IMetadataService>(0);

            // TODO get all streams
            Dictionary<int, QueueToStreamEntry> queueToStreamsMap;

            var streamProvider = GetStreamProvider("SMSProvider");
            stream = streamProvider.GetStream<string>(Constants.playerUpdatesStreamId, Constants.streamNamespace);
            return;

        }

        public async Task ReceiveEvent(string value)
        {
            // map


            // publish

            return;

        }
    }
}
