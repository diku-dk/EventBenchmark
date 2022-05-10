using System;
using System.Collections.Generic;
using Client.Configuration;

namespace Client.UseCases.eShop
{
    public interface IUseCaseConfig
    {

        List<string> GetTransactions();

        /**
         * Each entry represents a tranasction
         * Example: 3 entries, if one transaction for each entry, then each has 33% chance of being selected in a random selection
         */
        List<string> GetDistributionOfTransactions();

        List<TimeSpan> GetPeriodBetweenRequestsOfSameTransaction();

        List<int> GetNumberOfRequestsPerTransaction(); // 0 if no limit

        TimeSpan? TimeLimit(); // limit of time if applicable

        Distribution GetDistribution();

        Dictionary<string, string> GetUrlMap();

    }
}
