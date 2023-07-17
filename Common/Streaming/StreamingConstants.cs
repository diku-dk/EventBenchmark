using System;

namespace Common.Streaming
{
    public class StreamingConstants
    {

        /*
         * INFRA
         */

        public const string DefaultStreamStorage = "PubSubStore";

        public const string DefaultStreamProvider = "StreamProvider";

        /*
         * DRIVER
         */

        public const string CustomerWorkerNameSpace = "CustomerWorker";
        public const string SellerWorkerNameSpace = "SellerWorker";
        public const string DeliveryWorkerNameSpace = "DeliveryWorker";

        // public static readonly string TransactionNameSpace = "tx";

    }
}
