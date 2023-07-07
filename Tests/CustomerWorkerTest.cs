using Orleans.TestingHost;
using Marketplace.Test;

/*
 * if fails, add orleans.codegenerator.msbuild and core.abstractions
 */
namespace Tests
{
    [Collection(ClusterCollection.Name)]
    public class CustomerWorkerTest
    {
        private readonly TestCluster _cluster;

        private readonly Random random = new Random();

        public CustomerWorkerTest(ClusterFixture fixture)
        {
            this._cluster = fixture.Cluster;
        }

    }
}

