using Common.Distribution;

namespace Common.Experiment
{
	public class RunConfig
	{
        public int numProducts { get; set; }

        public DistributionType sellerDistribution { get; set; }

        public DistributionType keyDistribution { get; set; }
    }
}

