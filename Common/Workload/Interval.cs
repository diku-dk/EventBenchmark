using Orleans;

namespace Common.Workload
{
    public class Interval
    {
        public int min { get; set; }

        public int max { get; set; }

        public Interval(int min, int max)
        {
            this.min = min;
            this.max = max;
        }
    }
	
}