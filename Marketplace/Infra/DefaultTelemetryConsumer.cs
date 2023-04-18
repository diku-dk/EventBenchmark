using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Marketplace.Infra
{

    public class DefaultTelemetryConsumer : IExceptionTelemetryConsumer
    {

        private readonly ILogger<DefaultTelemetryConsumer> _logger;

        public DefaultTelemetryConsumer(ILogger<DefaultTelemetryConsumer> _logger)
		{
            this._logger = _logger;
            this._logger.LogError("[Marketplace] DefaultTelemetryConsumer created!");
        }

        public void Close()
        {
            // throw new NotImplementedException();
        }

        public void Flush()
        {
            // throw new NotImplementedException();
        }

        public void TrackException(Exception exception, IDictionary<string, string> properties = null, IDictionary<string, double> metrics = null)
        {
            this._logger.LogError("[Marketplace] DefaultTelemetryConsumer exception received: {0}", exception.Message);
        }
    }
}

