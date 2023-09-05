using System;
using Orleans.TestingHost;

namespace Marketplace.Test
{
    /**
     * https://learn.microsoft.com/en-us/dotnet/orleans/tutorials-and-samples/testing
     * 
     * 
     */
    public class ClusterFixture : IDisposable
    {
        public ClusterFixture()
        {
            var builder = new TestClusterBuilder();
            Cluster = builder.Build();
            Cluster.Deploy();
        }

        public void Dispose()
        {
            Cluster.StopAllSilos();
        }

        public TestCluster Cluster { get; private set; }
    }
}

