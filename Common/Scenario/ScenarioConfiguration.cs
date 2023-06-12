using System;
using System.Collections.Generic;
using Common.Configuration;
using Common.Scenario.Customer;
using Common.Scenario.Seller;
using Common.Streaming;
using Common.YCSB;

namespace Common.Scenario
{

    public enum SubmissionEnum
    {
        QUANTITY,
        TIME_IN_MILLI
    }

    public class ScenarioConfiguration
    {

        public SubmissionStrategy submissionStrategy = SubmissionStrategy.BURST_THEN_CONTROL;

        //
        public SubmissionEnum submissionType = SubmissionEnum.TIME_IN_MILLI;

        // how much time a window or burst may remain. in milliseconds
        public int submissionValue = 5000;

        public int waitBetweenSubmissions = 0;

        // a timer is configured to notify the orchestrator grain about the termination
        public int executionTime = 6000;

        // e.g. customer_session 70, price_update 75, 
        public IDictionary<TransactionType,int> transactionDistribution;

        // similar to ingestion config
        // but as new microservices might be added here
        // we have this attribute in the config
        public Dictionary<string, string> mapTableToUrl;

        // map kafka topic to orleans stream Guid
        public Dictionary<string, Guid> mapTopicToStreamGuid = new()
        {
            // seller
            ["low-stock-warning"] = StreamingConfiguration.SellerReactStreamId,
            // customer
            ["abandoned-cart"] = StreamingConfiguration.CustomerReactStreamId,
            ["payment-rejected"] = StreamingConfiguration.CustomerReactStreamId,
            ["out-of-stock"] = StreamingConfiguration.CustomerReactStreamId,
            ["price-update"] = StreamingConfiguration.CustomerReactStreamId,
            ["product-unavailable"] = StreamingConfiguration.CustomerReactStreamId,
        };

        public CustomerWorkerConfig customerWorkerConfig;

        public SellerWorkerConfig sellerWorkerConfig;

        // customer key distribution
        public Distribution customerDistribution;

    }
}
