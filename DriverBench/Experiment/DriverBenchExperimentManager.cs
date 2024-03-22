using Common.Experiment;
using Common.Workload;
using DuckDB.NET.Data;

namespace DriverBench.Experiment;

public sealed class DriverBenchExperimentManager : AbstractExperimentManager
{
    public DriverBenchExperimentManager(ExperimentConfig config, DuckDBConnection duckDBConnection) : base(config, duckDBConnection)
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

    protected override WorkloadManager SetUpManager(int runIdx)
    {
        throw new NotImplementedException();
    }

    protected override void TrimStreams()
    {
        throw new NotImplementedException();
    }

}

