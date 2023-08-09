using Common.Streaming;
using Common.Workload;
using Microsoft.AspNetCore.Mvc;
using Dapr;

namespace Daprr.Controllers;

[ApiController]
public class EventHandler : ControllerBase
{
    private const string PUBSUB_NAME = "pubsub";

    private readonly ILogger<EventHandler> logger;

    public EventHandler(ILogger<EventHandler> logger)
    {
        this.logger = logger;
    }

    private const string deleteMark = "TransactionMark_DELETE_PRODUCT";
    private const string updateMark = "TransactionMark_PRICE_UPDATE";
    private const string checkoutMark = "TransactionMark_CUSTOMER_SESSION";

    [HttpPost("/deleteMark")]
    [Topic(PUBSUB_NAME, deleteMark)]
    public async Task<ActionResult> ProcessDeleteMark([FromBody] TransactionMark deleteMark)
    {
        await Task.WhenAll(Shared.ResultQueue.Writer.WriteAsync(WorkloadManager.ITEM).AsTask(),
                            Shared.FinishedTransactionMarks.Writer.WriteAsync(deleteMark).AsTask());
        return Ok();
    }

    [HttpPost("/priceUpdateMark")]
    [Topic(PUBSUB_NAME, updateMark)]
    public async Task<ActionResult> ProcessPriceUpdateMark([FromBody] TransactionMark priceUpdateMark)
    {
        await Task.WhenAll(Shared.ResultQueue.Writer.WriteAsync(WorkloadManager.ITEM).AsTask(),
                        Shared.FinishedTransactionMarks.Writer.WriteAsync(priceUpdateMark).AsTask());
        return Ok();
    }

    [HttpPost("/checkoutMark")]
    [Topic(PUBSUB_NAME, checkoutMark)]
    public async Task<ActionResult> ProcessCheckoutMark([FromBody] TransactionMark checkoutMark)
    {
        await Task.WhenAll(Shared.ResultQueue.Writer.WriteAsync(WorkloadManager.ITEM).AsTask(),
                        Shared.FinishedTransactionMarks.Writer.WriteAsync(checkoutMark).AsTask());
        return Ok();
    }

}