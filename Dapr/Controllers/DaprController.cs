using System.Net;
using CartMS.Services;
using Common.Experiment;
using Microsoft.AspNetCore.Mvc;

namespace Dapr.Controllers;

[ApiController]
public class DaprController : ControllerBase
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ILogger<DaprController> logger;
    private readonly IExperimentService experimentService;

    public DaprController(IExperimentService experimentService, IHttpClientFactory httpClientFactory, ILogger<DaprController> logger)
    {
        this.experimentService = experimentService;
        this.httpClientFactory = httpClientFactory;
        this.logger = logger;
    }

    [Route("/runExperiment")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.Accepted)]
    public ActionResult Reset([FromBody] ExperimentConfig experimentConfig)
    {
        logger.LogWarning("Reset requested at {0}", DateTime.UtcNow);
        return Ok();
    }



}