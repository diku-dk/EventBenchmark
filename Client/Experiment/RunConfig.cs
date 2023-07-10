using Common.Distribution;

namespace Client.Experiment
{
	public class RunConfig
	{
        public int numProducts { get; set; }

        public DistributionType customerDistribution { get; set; }

        public DistributionType sellerDistribution { get; set; }

        public DistributionType keyDistribution { get; set; }
    }
}

