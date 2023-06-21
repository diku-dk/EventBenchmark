namespace Common.Distribution
{
    public enum DistributionType
    {

        NORMAL, // bell-shaped, a value near the center is more likely to occur as opposed to values on the tails
        UNIFORM, // every value in an interval from a to b is equally likely to occur. i.e., random
        NON_UNIFORM, // any probability distribution that is not uniform or evenly distributed
        ZIPFIAN

    }
}