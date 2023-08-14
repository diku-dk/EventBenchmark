using Orleans.TestingHost;
using Marketplace.Test;

namespace Tests;

[Collection(ClusterCollection.Name)]
public class CustomerThreadTest
{
    private readonly TestCluster _cluster;

    private readonly Random random = new Random();

    public CustomerThreadTest(ClusterFixture fixture)
    {
        this._cluster = fixture.Cluster;
    }

    // setup a 30 second test. setup a server to receive and deliver marks.

}


