using System;
using System.Collections.Generic;
using Orleans.Runtime;

namespace Client.Infra
{

    // FIXME it seems it is not receiving any message from the silo...

    /**
     * https://learn.microsoft.com/en-us/dotnet/orleans/host/monitoring/silo-error-code-monitoring
     */
    public class DefaultTelemetryConsumer : IExceptionTelemetryConsumer
    {
		public DefaultTelemetryConsumer()
		{
            Console.WriteLine("[DefaultTelemetryConsumer] Instance created!");
        }

        public void Close()
        {
            throw new NotImplementedException();
        }

        public void Flush()
        {
            throw new NotImplementedException();
        }

        public void TrackException(Exception exception, IDictionary<string, string> properties = null, IDictionary<string, double> metrics = null)
        {
            Console.WriteLine("[DefaultTelemetryConsumer] Exception received: {0}", exception.Message);
        }
    }
}

