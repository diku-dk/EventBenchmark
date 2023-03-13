using System;

namespace Common.YCSB
{
    internal class NormalGenerator : NumberGenerator
    {

        // https://stackoverflow.com/questions/218060/random-gaussian-variables
        public override double Mean()
        {
            throw new NotImplementedException();
        }

        public override long NextValue()
        {
            throw new NotImplementedException();
        }
    }
}
