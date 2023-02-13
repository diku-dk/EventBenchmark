using System;
namespace Common.YCSB
{
    public abstract class NumberGenerator
    {

        private long lastVal;

        protected Random random;

        public NumberGenerator()
        {
            this.random = new Random();
        }

        protected void SetLastValue(long last)
        {
            this.lastVal = last;
        }

        public long lastValue()
        {
            return this.lastVal;
        }

        public abstract double Mean();

        public abstract long NextValue();

    }
}
