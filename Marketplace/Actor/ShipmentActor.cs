using System;
using Marketplace.Entity;
using System.Threading.Tasks;
using System.Collections.Generic;
using Common.Scenario.Entity;
using Orleans;

namespace Marketplace.Actor
{

    public interface IShipmentActor : IGrainWithIntegerKey
    {
        //
        public Task DumbCall();

        public Task<Dictionary<long, decimal>> GetQuotation(string customerZipCode);

        public Task<decimal> GetQuotation(string from, string to);

        public Task ProcessShipment(Invoice invoice);

        public Task UpdateDeliveryStatus();
    }


    public class ShipmentActor : Grain, IShipmentActor
    {
		public ShipmentActor()
		{
		}

        public Task<Dictionary<long, decimal>> GetQuotation(string customerZipCode)
        {
            // from a table of combinations,  seller to another zipcode, build the cost for each item
            // then sum
            return null;
        }

        public Task ProcessShipment(Invoice invoice)
        {
            
        }

        public Task UpdateDeliveryStatus()
        {
            return null;
        }
    }
}

