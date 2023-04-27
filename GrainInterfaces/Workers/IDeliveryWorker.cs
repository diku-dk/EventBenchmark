using System;
using Common.Entity;
using Common.Scenario.Seller;
using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace GrainInterfaces.Workers
{
	public interface IDeliveryWorker : IGrainWithIntegerKey
    {
        public Task Init(string shipmentUrl);
	}
}

