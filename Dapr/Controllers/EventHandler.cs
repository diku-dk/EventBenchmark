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

    private const string productUpdateMark = "TransactionMark_UPDATE_PRODUCT";
    private const string priceUpdateMark = "TransactionMark_PRICE_UPDATE";
    private const string checkoutMark = "TransactionMark_CUSTOMER_SESSION";

    [HttpPost("/productUpdateMark")]
    [Topic(PUBSUB_NAME, productUpdateMark)]
    public async Task<ActionResult> ProcessProductUpdateMark([FromBody] TransactionMark productUpdateMark)
    {
        var ts = DateTime.UtcNow;
        await Shared.ResultQueue.Writer.WriteAsync(Shared.ITEM);
        if (productUpdateMark.status == MarkStatus.SUCCESS)
        {
            await Shared.ProductUpdateOutputs.Writer.WriteAsync(new(productUpdateMark.tid, ts));
        } else
        {
            logger.LogDebug("Received a poison TransactionMark_UPDATE_PRODUCT");
            await Shared.PoisonProductUpdateOutputs.Writer.WriteAsync(productUpdateMark);
        }
        return Ok();
    }

    [HttpPost("/priceUpdateMark")]
    [Topic(PUBSUB_NAME, priceUpdateMark)]
    public async Task<ActionResult> ProcessPriceUpdateMark([FromBody] TransactionMark priceUpdateMark)
    {
        var ts = DateTime.UtcNow;
        await Shared.ResultQueue.Writer.WriteAsync(Shared.ITEM);
        if (priceUpdateMark.status == MarkStatus.SUCCESS)
        {
            await Shared.PriceUpdateOutputs.Writer.WriteAsync(new(priceUpdateMark.tid, ts));
        }
        else
        {
            await Shared.PoisonPriceUpdateOutputs.Writer.WriteAsync(priceUpdateMark);
        }
        return Ok();
    }

    [HttpPost("/checkoutMark")]
    [Topic(PUBSUB_NAME, checkoutMark)]
    public async Task<ActionResult> ProcessCheckoutMark([FromBody] TransactionMark checkoutMark)
    {
        var ts = DateTime.UtcNow;
        await Shared.ResultQueue.Writer.WriteAsync(Shared.ITEM);
        if (checkoutMark.status == MarkStatus.SUCCESS)
        {
            await Shared.CheckoutOutputs.Writer.WriteAsync(new(checkoutMark.tid, ts));
        }
        else
        {
            await Shared.PoisonCheckoutOutputs.Writer.WriteAsync(checkoutMark);
        }
        return Ok();
    }

}