namespace Client.Workflow
{
	public class WorkflowConfig
	{
        // step
        // check whether the microservices are all active before starting the workflow
        public bool healthCheck { get; set; }

        // all microservices are expected to implement this API
        public const string healthCheckEndpoint = "/health";

        // step
        public bool dataLoad { get; set; }

        // step
        public bool ingestion { get; set; }

        // step
        public bool transactionSubmission { get; set; }

        // prometheus
        public bool collection { get; set; }

        // in future, constraint checking

        // submit requests to clean data created as part of the transaction submission e.g., orders, payments, shipments, carts, etc
        public bool cleanup { get; set; }

    }
}