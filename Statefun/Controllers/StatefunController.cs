using Common.Experiment;
using System.Net;
using Microsoft.AspNetCore.Mvc;

namespace Statefun.Controllers;

[ApiController]
public class StatefunController : ControllerBase
{

    private readonly ILogger<StatefunController> logger;

    // 0 for false, 1 for true.
    private static int usingResource = 0;

    public StatefunController(IHttpClientFactory httpClientFactory, ILogger<StatefunController> logger)
    {
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
            StatefunExperimentManager experimentManager = new StatefunExperimentManager(config);
            await experimentManager.Run();
            Interlocked.Exchange(ref usingResource, 0);
            return Ok();
        }
        return StatusCode((int)HttpStatusCode.MethodNotAllowed, "An experiment is in progress already");
    }


}