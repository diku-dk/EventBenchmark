using CartMS.Infra;
using Dapr.Client;
using Microsoft.Extensions.Options;

namespace CartMS.Services;

public class ExperimentService : IExperimentService
{

    private const string PUBSUB_NAME = "pubsub";

    private readonly DaprClient daprClient;

    private readonly DaprConfig config;
    private readonly ILogger<ExperimentService> logger;

    public ExperimentService(DaprClient daprClient, IOptions<DaprConfig> config, ILogger<ExperimentService> logger)
	{
        this.daprClient = daprClient;
        this.config = config.Value;
        this.logger = logger;
    }


}
