using System;
using Marketplace.Infra;
using Marketplace.Message;
using Orleans;
using System.Threading.Tasks;

namespace Marketplace.Interfaces
{
    public interface IPaymentActor : IGrainWithIntegerKey, SnapperActor
    {
        public Task ProcessPayment(Invoice invoice);
    }
}

