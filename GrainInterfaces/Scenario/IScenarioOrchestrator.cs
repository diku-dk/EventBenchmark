using System;
using Common.Scenario;
using System.Threading.Tasks;
using Orleans;

namespace GrainInterfaces.Scenario
{
	public interface IScenarioOrchestrator : IGrainWithStringKey
	{

        public Task Init(ScenarioConfiguration scenarioConfiguration);


    }
}

