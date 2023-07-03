namespace Client.Workflow
{
	public class WorkflowConfig
	{
        // step
        // check whether the microservices are all active before starting the workflow
        public bool healthCheck = true;

        // all microservices are expected to implement this API
        public const string healthCheckEndpoint = "/health";

        // step
        public bool dataLoad = true;

        // step
        public bool ingestion = true;

        // step
        public bool transactionSubmission = true;

        // prometheus
        public bool collection = true;

        // in future, constraint checking

        // submit requests to clean data created as part of the transaction submission e.g., orders, payments, shipments, carts, etc
        public bool cleanup = false;

        // all microservices are expected to implement this API
        public string cleanupEndpoint = "/cleanup";
    }
}