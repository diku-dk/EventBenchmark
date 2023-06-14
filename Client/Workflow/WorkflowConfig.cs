using System;
namespace Client.Workflow
{
	public class WorkflowConfig
	{
        // step
        // check whether the microservices are all active before starting the workflow
        public bool healthCheck = true;

        public string healthCheckEndpoint = "/health";

        // step
        public bool dataLoad = true;

        // step
        public bool ingestion = true;

        // step
        public bool transactionSubmission = true;

        // in future, constraint checking

        // submit requests to clean data created as part of the transaction submission e.g., orders, payments, shipments, carts, etc
        public bool cleanup = false;

        public string cleanupEndpoint = "/cleanup";
    }
}

