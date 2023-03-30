using System;
using Common.Scenario.Entity;
using Orleans;
using System.Threading.Tasks;
using Marketplace.Entity;

namespace Marketplace.Actor
{

    public interface IOrderActor : IGrainWithIntegerKey
    {

        // internal Task Checkout(Checkout checkout);


    }

    public class OrderActor : Grain, IOrderActor
	{
		public OrderActor()
		{
		}

        private Task Checkout(Checkout checkout)
        {
            throw new NotImplementedException();
        }
    }
}

