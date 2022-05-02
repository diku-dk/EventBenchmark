using System;
namespace Common.YCSB
{
    public abstract class NumberGenerator
    {

        private Int64 lastVal;

        protected Random random;

        public NumberGenerator()
        {
            this.random = new Random();
        }

        protected void SetLastValue(Int64 last)
        {
            this.lastVal = last;
        }

        public Int64 lastValue()
        {
            return this.lastVal;
        }

        public abstract double Mean();

        public abstract long NextValue();

    }
}
