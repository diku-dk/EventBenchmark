using System;
namespace Marketplace.Infra
{
	public class ActorSettings
	{
		public int numOrderPartitions { get; set; }
        public int numPaymentPartitions { get; set; }
        public int numShipmentPartitions { get; set; }
        public int numProductPartitions { get; set; }
        public int numStockPartitions { get; set; }
        public int numCustomerPartitions { get; set; }

        public static ActorSettings GetDefault()
        {
            return new()
            {
                numCustomerPartitions = 1,
                numOrderPartitions = 1,
                numPaymentPartitions = 1,
                numProductPartitions = 1,
                numShipmentPartitions = 1,
                numStockPartitions = 1
            };
        }

    }
}

