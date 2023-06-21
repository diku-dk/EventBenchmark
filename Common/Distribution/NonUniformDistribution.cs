using System;
using Common.Distribution.YCSB;

namespace Common.Distribution
{
    /**
     * https://www.tpc.org/information/sessions/sigmod/sld011.htm
     */
    public class NonUniformDistribution : NumberGenerator
	{
        private readonly int A;
        private readonly int x;
        private readonly int y;
        private readonly Random rnd;

        public NonUniformDistribution(int A, int x, int y)
		{
            this.A = A;
            this.x = x;
            this.y = y;
            this.rnd = new Random();
        }

        public override double Mean()
        {
            throw new NotImplementedException();
        }

        public override long NextValue()
        {
            var part1 = rnd.Next(0, A + 1);
            var part2 = rnd.Next(x, y + 1);
            return ((part1 | part2) % (y - x + 1)) + x;
        }

    }
}

