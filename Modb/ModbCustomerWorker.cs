using System.Net.Http;
using Common.Entities;
using Common.Infra;
using Common.Services;
using Common.Workers.Customer;
using Common.Workload.CustomerWorker;
using Microsoft.Extensions.Logging;

namespace Modb;

public sealed class ModbCustomerWorker : DefaultCustomerWorker
{
	private ModbCustomerWorker(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, httpClient, logger)
    {
        
    }

    public static new ModbCustomerWorker BuildCustomerWorker(IHttpClientFactory httpClientFactory, ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("ModbCustomer" + customer.id.ToString());
        return new ModbCustomerWorker(sellerService, numberOfProducts, config, customer, httpClientFactory.CreateClient(), logger);
    }

    protected override string BuildCheckoutUrl()
    {
        return this.config.checkoutUrl;
    }

    protected override void DoAfterSuccessSubmission(string tid){ }

}

