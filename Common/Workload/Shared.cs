using System.Threading.Channels;
using Common.Streaming;
using Common.Workload.Metrics;

namespace Common.Workload
{
	public sealed class Shared
	{

        public static readonly Channel<TransactionOutput> ProductUpdateOutputs = Channel.CreateUnbounded<TransactionOutput>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<TransactionMark> PoisonProductUpdateOutputs = Channel.CreateUnbounded<TransactionMark>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<TransactionOutput> PriceUpdateOutputs = Channel.CreateUnbounded<TransactionOutput>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<TransactionMark> PoisonPriceUpdateOutputs = Channel.CreateUnbounded<TransactionMark>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<TransactionOutput> CheckoutOutputs = Channel.CreateUnbounded<TransactionOutput>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<TransactionMark> PoisonCheckoutOutputs = Channel.CreateUnbounded<TransactionMark>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });

        public static readonly Channel<byte> ResultQueue = Channel.CreateUnbounded<byte>(new UnboundedChannelOptions()
        {
            SingleWriter = false,
            SingleReader = true,
            AllowSynchronousContinuations = false,
        });
    }
}