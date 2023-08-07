using CartMS.Services;
using Common.Streaming;
using Common.Workload;
using Dapr;
using Dapr.Client;
using Microsoft.AspNetCore.Mvc;
using System.Text;

namespace CartMS.Controllers;

[ApiController]
public class EventHandler : ControllerBase
{
    private const string PUBSUB_NAME = "pubsub";

    private readonly DaprClient daprClient;

    private readonly ILogger<EventHandler> logger;

    public EventHandler(DaprClient daprClient,
                            ILogger<EventHandler> logger)
    {
        this.daprClient = daprClient;
        this.logger = logger;
    }

    private const string deleteMark = "TransactionMark_DELETE_PRODUCT";
    private const string updateMark = "TransactionMark_PRICE_UPDATE";
    private const string checkoutMark = "TransactionMark_CUSTOMER_SESSION";

    [HttpPost("/deleteMark")]
    [Topic(PUBSUB_NAME, deleteMark)]
    public async Task<ActionResult> ProcessDeleteMark([FromBody] TransactionMark deleteMark)
    {
        await Task.Delay(100);
        return Ok();
    }

}