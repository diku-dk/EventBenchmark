using System;
using System.Collections.Concurrent;
using System.Net.Http;
using Common.Entities;
using Common.Http;
using Common.Infra;
using Common.Services;
using Common.Streaming;
using Common.Workers.Customer;
using Common.Workload;
using Common.Workload.CustomerWorker;
using Microsoft.Extensions.Logging;

namespace Monitor;

/**
 * Implements the functionality of a synchronous customer API.
 * As a result, this class must add a finished transaction mark in the DoAfterSubmission method
 */
public sealed class MonitorCustomerWorker : DefaultCustomerWorker
{

    public readonly BlockingCollection<object> mailbox;

    private MonitorCustomerWorker(ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer, HttpClient httpClient, ILogger logger) : base(sellerService, numberOfProducts, config, customer, httpClient, logger)
    { }

    public static new MonitorCustomerWorker BuildCustomerWorker(
        BlockingCollection<object> mailbox,
        ISellerService sellerService, int numberOfProducts, CustomerWorkerConfig config, Customer customer)
    {
        var logger = LoggerProxy.GetInstance("Customer_" + customer.id.ToString());
        return new MonitorCustomerWorker(sellerService, numberOfProducts, config, customer, null, logger);
    }

    protected override void SendCheckoutRequest(string tid)
    {
        string objStr = this.BuildCheckoutPayload(tid);
        StringContent payload = HttpUtils.BuildPayload(objStr);
        string url = this.BuildCheckoutUrl();
        int maxAttempts = this.GetMaxCheckoutAttempts();
        DateTime sentTs = DateTime.UtcNow;
        int attempt = 1;
        try
        {
            // send to actual synthetic cart worker
            this.mailbox.Add(payload);
            // queue the payload to the specific cart actor


            // sender possess/maintains the input queue of every cart actor
            // and sends the payload to this queue
            // i.e., asynchronous queuing
            // by the time the message is queued, we dont need to wait or synchronize with the receiver

            Boolean success = true;
            if(success)
            {
                this.DoAfterSuccessSubmission(tid);
                this.submittedTransactions.Add(new(tid, TransactionType.CUSTOMER_SESSION, sentTs));
            } else
            {
                this.abortedTransactions.Add(new TransactionMark(tid, TransactionType.CUSTOMER_SESSION, this.customer.id, MarkStatus.ABORT, "cart"));
            }
        }
        catch (Exception e)
        {
            this.logger.LogError("Customer {0} Url {1}: Exception: {2} Message: {3} ", this.customer.id, url, e.GetType().Name, e.Message);
            this.InformFailedCheckout();
        }
    }

}

