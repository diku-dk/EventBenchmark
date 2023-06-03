using System;
using Client.DataGeneration;
using Client.DataGeneration.Real;
using Common.Ingestion.Config;
using Common.Scenario;
using Orleans;

namespace Client.Execution
{
	public class MasterConfiguration
	{
        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        public WorkflowConfig workflowConfig = null;

        public SyntheticDataSourceConfiguration syntheticDataConfig = null;

        public OlistDataSourceConfiguration olistDataConfig = null;

        public IngestionConfiguration ingestionConfig = null;

        public ScenarioConfiguration scenarioConfig;

    }
}

