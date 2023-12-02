using System;
using Common.Experiment;
using Common.Workload;
using Common.Infra;
using Common.Entities;
using Common.Services;
using Common.Workers;
using Common.Workers.Seller;
using Common.Workers.Customer;
using Statefun.Metric; 
using DuckDB.NET.Data;
using Statefun.Workers;
using System.Net.Http;
using System.Text.Json;
using Common.Streaming;
using Common.Workload.Metrics;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Statefun.Workload;

public class StatefunReceiptPullingThread 
{
    protected readonly ISellerService sellerService;

    protected readonly ICustomerService customerService;

    protected readonly IDeliveryService deliveryService;

    private string url;

    public StatefunReceiptPullingThread(String url, ICustomerService customerService, ISellerService sellerService, IDeliveryService deliveryService) {
        this.url = url;
        this.customerService = customerService;
        this.sellerService = sellerService;
        this.deliveryService = deliveryService;           
    }

    public async Task Run(CancellationToken cancellationToken) {        
        int i = 0;
        using (var httpClient = new HttpClient()) {            
            while (!cancellationToken.IsCancellationRequested) {
               try {                    
                    i = i + 1;                    
                    HttpResponseMessage response = await httpClient.GetAsync(url);                    
                    if (response.IsSuccessStatusCode)
                    {                                                    
                        DateTime endTime = DateTime.UtcNow;       
                        string responseBody = await response.Content.ReadAsStringAsync();  
                        if (string.IsNullOrEmpty(responseBody)) {                            
                            continue;
                        }
                        JObject jsonObject = JObject.Parse(responseBody);
                        TransactionMark transactionMark = JsonConvert.DeserializeObject<TransactionMark>(jsonObject.ToString());

                        await Shared.ResultQueue.Writer.WriteAsync(WorkloadManager.ITEM);

                        TransactionOutput transactionOutput = new TransactionOutput(transactionMark.tid, endTime);
                        int actorId = transactionMark.actorId;                                                
                        
                        switch (transactionMark.type) {
                            case TransactionType.CUSTOMER_SESSION:
                                this.customerService.AddFinishedTransaction(actorId, transactionOutput);
                                if (transactionMark.status == MarkStatus.SUCCESS) 
                                    await Shared.CheckoutOutputs.Writer.WriteAsync(transactionOutput);
                                else
                                    await Shared.PoisonCheckoutOutputs.Writer.WriteAsync(transactionMark);
                                break;
                            case TransactionType.PRICE_UPDATE:
                                this.sellerService.AddFinishedTransaction(actorId, transactionOutput);
                                if (transactionMark.status == MarkStatus.SUCCESS) 
                                    await Shared.PriceUpdateOutputs.Writer.WriteAsync(transactionOutput);
                                else
                                    await Shared.PoisonPriceUpdateOutputs.Writer.WriteAsync(transactionMark);
                                break;
                            case TransactionType.UPDATE_PRODUCT:
                                this.sellerService.AddFinishedTransaction(actorId, transactionOutput);
                                if (transactionMark.status == MarkStatus.SUCCESS) 
                                    await Shared.ProductUpdateOutputs.Writer.WriteAsync(transactionOutput);
                                else
                                    await Shared.PoisonProductUpdateOutputs.Writer.WriteAsync(transactionMark);
                                break;
                            case TransactionType.QUERY_DASHBOARD:
                                this.sellerService.AddFinishedTransaction(actorId, transactionOutput);
                                if (transactionMark.status == MarkStatus.SUCCESS) 
                                    await Shared.DashboardQueryOutputs.Writer.WriteAsync(transactionOutput);
                                else
                                    await Shared.PoisonDashboardQueryOutputs.Writer.WriteAsync(transactionMark);
                                break;
                            case TransactionType.UPDATE_DELIVERY:
                                this.deliveryService.AddFinishedTransaction(transactionOutput);
                                if (transactionMark.status == MarkStatus.SUCCESS) 
                                    await Shared.DeliveryUpdateOutputs.Writer.WriteAsync(transactionOutput);
                                else
                                    await Shared.PoisonDeliveryUpdateOutputs.Writer.WriteAsync(transactionMark);
                                break;                            
                            default:
                                throw new Exception("Unknown transaction type: " + transactionMark.type);
                        }                                                            
                    }
                    else
                    {
                        Console.WriteLine("Pulling Request to receipts failed with status code: " + response.StatusCode);
                    }                    
                    
               } catch (Exception e) {
                   Console.WriteLine("exception: "+ e.Message);
               }
           }
        }
    }
}