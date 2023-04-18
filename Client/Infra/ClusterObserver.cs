using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans;
using Orleans.Runtime;

namespace Client.Infra
{

    /**
     * 
     */
    public class ClusterObserver : ILifecycleParticipant<IClusterClientLifecycle>, ILifecycleObserver
    {

        public readonly static TaskCompletionSource<bool> _siloFailedTask = new TaskCompletionSource<bool>();

        private readonly ILogger<ClusterObserver> _logger;

        public ClusterObserver(ILogger<ClusterObserver> _logger)
		{
            this._logger = _logger;
            this._logger.LogWarning("[DefaultTelemetryConsumer] Instance created!");
        }

        public Task OnStart(CancellationToken cancellationToken)
        {
            this._logger.LogWarning("[DefaultTelemetryConsumer] OnStart!");
            return Task.CompletedTask;
        }

        public Task OnStop(CancellationToken cancellationToken)
        {
            this._logger.LogWarning("[DefaultTelemetryConsumer] OnStop!");
            bool dead = cancellationToken.IsCancellationRequested ? true : false;

            if (dead)
            {
                this._logger.LogError("[DefaultTelemetryConsumer] Silo is dead!");
                _siloFailedTask.TrySetResult(true);
            }
            return Task.CompletedTask;
        }

        public void Participate(IClusterClientLifecycle lifecycle)
        {
            lifecycle.Subscribe(nameof(ClusterObserver),
                                ServiceLifecycleStage.RuntimeGrainServices,
                                this);
        }

    }
}

