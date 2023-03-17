using System;
using System.Collections.Generic;
using Common.Configuration;

namespace Common.Streaming
{
    public class StreamingConfiguration
    {

        /*
         * INFRA
         */

        public const string DefaultStreamStorage = "PubSubStore";

        public const string DefaultStreamProvider = "SMSProvider";

        public const string KafkaService = "localhost:9092";

        public const string ZooKeeperService = "localhost:2181";

        /*
         * DRIVER
         */
        public static readonly Guid IngestionStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC5");
        public static readonly Guid IngestionWorkerStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC6");

        public static readonly Guid WorkloadStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC7");


        /*
         * APP
         */
        public static readonly Guid CustomerStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC7");
    }
}
