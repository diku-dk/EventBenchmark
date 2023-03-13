using System;
using Common.Scenario;
using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Scenario
{
	public interface IScenarioOrchestrator : IGrainWithIntegerKey
	{

        public Task Run(ScenarioConfiguration scenarioConfiguration);


    }
}

