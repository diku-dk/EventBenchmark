using Common.Scenario;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace GrainInterfaces.Scenario
{
	public interface IScenarioOrchestrator : IGrainWithIntegerKey
	{

        public Task Start(ScenarioConfiguration scenarioConfiguration);

        public Task Stop();

    }
}

