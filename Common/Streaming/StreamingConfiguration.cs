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

        public const string defaultStreamStorage = "PubSubStore";

        public const string defaultStreamProvider = "SMSProvider";

        public const bool FireAndForgetDelivery = false;

        public const string kafkaService = "localhost:9092";

        public const string zooKeeperService = "localhost:2181";

        /*
         * APP
         */
        public static readonly Guid appStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC5");

    }
}
