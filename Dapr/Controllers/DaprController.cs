using System.Net;
using Common.Experiment;
using Common.Http;
using Common.Infra;
using Common.Ingestion;
using Daprr.Workload;
using DuckDB.NET.Data;
using Microsoft.AspNetCore.Mvc;

namespace Daprr.Controllers;

[ApiController]
public class DaprController : ControllerBase
{
    private readonly IHttpClientFactory httpClientFactory;
    private readonly ILogger<DaprController> logger;

    // requests are served by different threads
    // to ensure visibility after updates, need interlocked
    private static ExperimentConfig config;
    private static DuckDBConnection connection;

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
        Interlocked.Exchange(ref connection, ConsoleUtility.GenerateData(config));
        return Ok("Data generated");
    }

    private static bool ConnectionIsSet()
    {
        if(connection is null){
            if(config.connectionString.SequenceEqual("DataSource=:memory:"))
            {
                return false;
            }
            else
            {
                Interlocked.Exchange(ref connection, new DuckDBConnection(config.connectionString));
                connection.Open();
                return true;
            }
        }
        return true;
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
        if (!ConnectionIsSet())
        {
            return BadRequest("Please generate some data first by selecting option 1.");
        }
        await IngestionOrchestratorV1.Run(connection, config.ingestionConfig);
        return Ok("Data ingested");
    }

    [Route("/3")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public ActionResult RunExperiment()
    {
        if(config is null)
        {
            return BadRequest("Please register a configuration first.");
        }
        if (!ConnectionIsSet())
        {
            return BadRequest("Please generate some data first by selecting option 1.");
        }
        DaprExperimentManager experimentManager = DaprExperimentManager.BuildDaprExperimentManager(this.httpClientFactory, config, connection);
        experimentManager.RunSimpleExperiment();
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
        if (!ConnectionIsSet())
        {
            return BadRequest("Please generate some data first by selecting option 1.");
        }
        await CustomIngestionOrchestrator.Run(connection, config.ingestionConfig);
        var expManager = DaprExperimentManager
                        .BuildDaprExperimentManager(new CustomHttpClientFactory(), config, connection);
        expManager.RunSimpleExperiment();
        return Ok("Experiment finished");
    }

    [Route("/5")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public ActionResult ParseNewConfiguration([FromBody] ExperimentConfig newConfig)
    {
        // Console.WriteLine("Parse new configuration from body requested. Is null? "+config is null);
        Interlocked.Exchange(ref config, newConfig);
        return Ok("New configuration parsed.");
    }

    [Route("/6/{path}")]
    [HttpPost]
    [ProducesResponseType((int)HttpStatusCode.OK)]
    public ActionResult ParseNewConfigurationFromPath(string path)
    {
        // Console.WriteLine("Parse new configuration from path requested. Is null? "+config is null);
        path = path.Replace("%2F","/");
        ExperimentConfig newConfig = ConsoleUtility.BuildExperimentConfig_(path);
        Interlocked.Exchange(ref config, newConfig);
        return Ok("New configuration parsed.");
    }

}