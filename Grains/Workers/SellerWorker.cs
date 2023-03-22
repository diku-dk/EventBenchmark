using System;
using System.Threading.Tasks;

namespace Grains.Workers
{
	public class SellerWorker
	{
       
        //

        // driver will call
        public void DeleteProduct()
        {

        }

        // driver will call
        public void UpdatePrice()
        {

        }

        // app will send
        // low stock, marketplace offer this estimation
        // received from a continuous query (basically implement a model)[defined function or sql]
        // the standard deviation for the continuous query
        private Task ReactToLowStock(string outOfStockEvent)
        {
            // given a distribution, the seller can increase and send an event increasing the stock or not
            // maybe it is reasonable to always increase stock to a minimum level
            // let's do it first
            return Task.CompletedTask;
        }
    }
}

