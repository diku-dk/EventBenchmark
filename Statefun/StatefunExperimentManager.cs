using Common.Experiment;
using Common.Workload;

namespace Statefun;

public class StatefunExperimentManager : ExperimentManager
{
    public StatefunExperimentManager(ExperimentConfig config) : base(config)
    {
    }

    protected override void Collect(int runIdx, DateTime startTime, DateTime finishTime)
    {
        throw new NotImplementedException();
    }

    protected override void PostExperiment()
    {
        throw new NotImplementedException();
    }

    protected override void PostRunTasks(int runIdx, int lastRunIdx)
    {
        throw new NotImplementedException();
    }

    protected override void PreExperiment()
    {
        throw new NotImplementedException();
    }

    protected override void PreWorkload(int runIdx)
    {
        throw new NotImplementedException();
    }

    protected override void RunIngestion()
    {
        throw new NotImplementedException();
    }

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        throw new NotImplementedException();
    }

    protected override void TrimStreams()
    {
        throw new NotImplementedException();
    }
}


