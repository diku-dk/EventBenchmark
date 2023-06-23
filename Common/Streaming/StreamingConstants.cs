using System;

namespace Common.Streaming
{
    public class StreamingConstants
    {

        /*
         * INFRA
         */

        public const string DefaultStreamStorage = "PubSubStore";

        public const string DefaultStreamProvider = "SMSProvider";

        /*
         * DRIVER
         */
        public static readonly Guid IngestionStreamId =       new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC1");
        public static readonly Guid IngestionWorkerStreamId = new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC2");

        public static readonly Guid WorkloadStreamId =        new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC3");
        public static readonly Guid CustomerStreamId =        new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC4");
        public static readonly Guid SellerStreamId   =        new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC5");
        public static readonly Guid DeliveryStreamId =        new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC6");

        /*
         * APP
         */
        public static readonly Guid CustomerReactStreamId =   new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC8");
        public static readonly Guid SellerReactStreamId   =   new("AD713788-B5AE-49FF-8B2C-F311B9CB0CC9");

        public static readonly string TransactionStreamNameSpace = "tx";

    }
}
