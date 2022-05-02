using System;
namespace Common.Configuration
{
    // a map from the external queue to internal stream
    public class QueueToStreamEntry
    {

        private readonly Guid streamId;
        private readonly string streamNamespace;
        private readonly string queue;

        public QueueToStreamEntry()
        {
        }
    }
}
