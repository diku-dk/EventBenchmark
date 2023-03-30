using System;
using Marketplace.Entity;
using System.Threading.Tasks;

namespace Marketplace.Actor
{
	public class StockActor
	{
		public StockActor()
		{
		}

        private Task Reserve(long productId, int quantity)
        {
            throw new NotImplementedException();
        }

        private Task CancelReserve(long productId, int quantity)
        {
            throw new NotImplementedException();
        }

        private Task Deduct(long productId, int quantity)
        {
            throw new NotImplementedException();
        }

    }
}

