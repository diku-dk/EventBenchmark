using System;
using Marketplace.Entity;
using System.Threading.Tasks;
using Orleans;

namespace Marketplace.Actor
{

    public interface IPaymentActor : IGrainWithIntegerKey
    {

    }

    public class PaymentActor : Grain, IPaymentActor
	{
		public PaymentActor()
		{
		}

        private Task ProcessPayment(Order order)
        {
            throw new NotImplementedException();
        }

    }
}

