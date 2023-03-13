using System;
namespace Common.YCSB
{
    public class UniformLongGenerator : NumberGenerator
    {

        private long lb;
        private long ub;
        private long interval;

        public UniformLongGenerator(long lb, long ub)
        {
            this.lb = lb;
            this.ub = ub;
            this.interval = this.ub - this.lb + 1L;
        }

        public override long NextValue()
        {
            long ret = Math.Abs(random.Next()) % this.interval + this.lb;
            this.SetLastValue(ret);
            return ret;
        }

        public override double Mean()
        {
            return (this.lb + this.ub) / 2.0D;
        }

        
    
    }
}
