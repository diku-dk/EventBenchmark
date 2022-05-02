using System;
namespace Common.YCSB
{
    public class ZipfianGenerator : NumberGenerator
    {

        public static readonly double ZIPFIAN_CONSTANT = 0.99D;

        private readonly bool allowitemcountdecrease;
        private readonly long items;
        private readonly long base_;
        private readonly double zipfianconstant;
        private double theta;
        private double zeta2theta;
        private double alpha;
        private double zetan;
        private long countforzeta;
        private double eta;


        private readonly object syncLock = new();

        public ZipfianGenerator(long items) : this(0L, items - 1L)
        {
            
        }

        public ZipfianGenerator(long min, long max) : this(min, max, ZIPFIAN_CONSTANT)
        {
            
        }

        public ZipfianGenerator(long items, double zipfianconstant) : this(0L, items - 1L, zipfianconstant)
        {
            
        }

        public ZipfianGenerator(long min, long max, double zipfianconstant) : this(min, max, zipfianconstant, zetastatic(max - min + 1L, zipfianconstant))
        {
            
        }

        private double zeta(long n, double thetaVal)
        {
            this.countforzeta = n;
            return zetastatic(n, thetaVal);
        }

        private static double zetastatic(long n, double theta)
        {
            return zetastatic(0L, n, theta, 0.0D);
        }

        private static double zetastatic(long st, long n, double theta, double initialsum)
        {
            double sum = initialsum;

            for (long i = st; i < n; ++i)
            {
                sum += 1.0D / Math.Pow((i + 1L), theta);
            }

            return sum;
        }

        private double zeta(long st, long n, double thetaVal, double initialsum)
        {
            this.countforzeta = n;
            return zetastatic(st, n, thetaVal, initialsum);
        }

        public ZipfianGenerator(long min, long max, double zipfianconstant, double zetan)
        {
            this.allowitemcountdecrease = false;
            this.items = max - min + 1L;
            this.base_ = min;
            this.zipfianconstant = zipfianconstant;
            this.theta = this.zipfianconstant;
            this.zeta2theta = this.zeta(2L, this.theta);
            this.alpha = 1.0D / (1.0D - this.theta);
            this.zetan = zetan;
            this.countforzeta = this.items;
            this.eta = ((1.0D - Math.Pow(2.0D / items, 1.0D - this.theta)) / (1.0D - this.zeta2theta / this.zetan));
            this.NextValue();
        }

        private long nextLong(long itemcount)
        {
            if (itemcount != this.countforzeta)
            {
                // no synchronize keyword in C#
                lock(syncLock) {
                    if (itemcount > this.countforzeta)
                    {
                        this.zetan = this.zeta(this.countforzeta, itemcount, this.theta, this.zetan);
                        this.eta = (1.0D - Math.Pow(2.0D / (double)this.items, 1.0D - this.theta)) / (1.0D - this.zeta2theta / this.zetan);
                    }
                    else if (itemcount < this.countforzeta && this.allowitemcountdecrease)
                    {
                        // System.err.println("WARNING: Recomputing Zipfian distribtion. This is slow and should be avoided. (itemcount=" + itemcount + " countforzeta=" + this.countforzeta + ")");
                        this.zetan = this.zeta(itemcount, this.theta);
                        this.eta = (1.0D - Math.Pow(2.0D / (double)this.items, 1.0D - this.theta)) / (1.0D - this.zeta2theta / this.zetan);
                    }
                }
            }

            double u = random.NextDouble();
            double uz = u * this.zetan;
            if (uz < 1.0D)
            {
                return this.base_;
            }
            else if (uz < 1.0D + Math.Pow(0.5D, this.theta))
            {
                return this.base_ + 1L;
            }
            else
            {
                long ret = (this.base_ + (long)(itemcount * Math.Pow(this.eta * u - this.eta + 1.0D, this.alpha)));
                this.SetLastValue(ret);
                return ret;
            }
        }

        public override long NextValue()
        {
            return this.nextLong(this.items);
        }

        public override double Mean()
        {
            throw new ApplicationException("TODO implement mean()");
        }



    }
}
