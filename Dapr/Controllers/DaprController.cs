using System.Net;
using Common.Experiment;
using Common.Http;
using Common.Infra;
using Daprr.Workload;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Mvc;

namespace Daprr.Controllers;

[ApiController]
public class DaprController : ControllerBase
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ILogger<DaprController> logger;

    private static ExperimentConfig config;
    private DuckDBConnection connection;

    public DaprController(IHttpClientFactory httpClientFactory, ILogger<DaprController> logger)
    {
        this.httpClientFactory = httpClientFactory;
        this.logger = logger;
    }

    [Route("/1")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public ActionResult GenerateData()
    {
        if(config is null)
        {
            return BadRequest("Please register a configuration first.");
        }
        this.connection = ConsoleUtility.GenerateData(config);
        return Ok("Data generated");
    }

    [Route("/2")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    [ProducesResponseType((int)HttpStatusCode.BadRequest)]
    public async Task<ActionResult> IngestData()
    {
        if(config is null)
        {
            return BadRequest("Please register a configuration first.");
        }
        if(this.connection is null){
            if(config.connectionString.SequenceEqual("DataSource=:memory:"))
            {
                return BadRequest("Please generate some data first by selecting option 1.");
            }
            else
            {
                this.connection = new DuckDBConnection(config.connectionString);
                this.connection.Open();
            }
        }
        await CustomIngestionOrchestrator.Run(this.connection, config.ingestionConfig);
        return Ok("Data ingested");
    }

    [Route("/3")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public async Task<ActionResult> RunExperiment()
    {
        if(config is null)
        {
            return BadRequest("Please register a configuration first.");
        }
        if (this.connection is null)
        {
            if (config.connectionString.SequenceEqual("DataSource=:memory:"))
            {
                return BadRequest("Please generate some data first by selecting option 1.");
            }
            else
            {
                this.connection = new DuckDBConnection(config.connectionString);
                this.connection.Open();
            }
        }
        DaprExperimentManager experimentManager = DaprExperimentManager.BuildDaprExperimentManager(this.httpClientFactory, config, this.connection);
        await experimentManager.Run();
        return Ok("Experiment finished");
    }

    [Route("/4")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    [ProducesResponseType((int)HttpStatusCode.BadRequest)]
    public async Task<ActionResult> IngestDataAndRunExperiment()
    {
        if(config is null)
        {
            return BadRequest("Please register a configuration first.");
        }
        if(this.connection is null){
            if(config.connectionString.SequenceEqual("DataSource=:memory:"))
            {
                return BadRequest("Please generate some data first by selecting option 1.");
            }
            else
            {
                this.connection = new DuckDBConnection(config.connectionString);
                this.connection.Open();
            }
        }
        await CustomIngestionOrchestrator.Run(this.connection, config.ingestionConfig);
        var expManager = DaprExperimentManager
                        .BuildDaprExperimentManager(new CustomHttpClientFactory(), config, this.connection);
        expManager.RunSimpleExperiment();
        return Ok("Experiment finished");
    }

    [Route("/5")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public ActionResult ParseNewConfiguration([FromBody] ExperimentConfig newConfig)
    {
        Console.WriteLine("Parse new configurarion requested. Is null? "+config is null);
        Interlocked.Exchange(ref config, newConfig);
        return Ok("New configuration parsed.");
    }

}