using System;
using Common.Ingestion;
using System.Threading.Tasks;
using Orleans;
using Common.Customer;

namespace GrainInterfaces.Workers
{
	public interface ICustomerWorker : IGrainWithIntegerKey
	{ 
		public Task Run(CustomerConfiguration config);
	}
}
