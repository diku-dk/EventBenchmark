using System.Net;
using Common.Experiment;
using Dapr.Workload;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Mvc;

namespace Common.Controllers;

[ApiController]
public class DaprController : ControllerBase
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ILogger<DaprController> logger;

    private DuckDBConnection connection;

    // 0 for false, 1 for true.
    private static int usingResource = 0;

    public DaprController(IHttpClientFactory httpClientFactory, ILogger<DaprController> logger)
    {
        this.httpClientFactory = httpClientFactory;
        this.logger = logger;
    }

    [Route("/runExperiment")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.Accepted)]
    public async Task<ActionResult> RunExperiment([FromBody] ExperimentConfig config)
    {
        // 0 indicates that the method is not in use.
        if (0 == Interlocked.Exchange(ref usingResource, 1))
        {
            logger.LogInformation("Request for experiment run accepted.");
            connection = new DuckDBConnection(config.connectionString);
            connection.Open();
            DaprExperimentManager experimentManager = DaprExperimentManager.BuildDaprExperimentManager(httpClientFactory, config, connection);
            await experimentManager.Run();
            Interlocked.Exchange(ref usingResource, 0);
            return Ok();
        }
        return StatusCode((int)HttpStatusCode.MethodNotAllowed, "An experiment is in progress already");
    }
    // https://learn.microsoft.com/en-us/dotnet/api/system.threading.interlocked?view=net-7.0


}