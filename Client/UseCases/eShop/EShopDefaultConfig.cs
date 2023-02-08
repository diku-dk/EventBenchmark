using System;
using System.Collections.Generic;
using Common.Configuration;
using Client.UseCases.eShop.Transactions;

namespace Client.UseCases.eShop
{
    public class EShopDefaultConfig : IUseCaseConfig
    {
        public EShopDefaultConfig()
        {
        }

        public List<int> GetPercentageOfTransactions()
        {
            return new List<int> { 100, 0, 0 };
        }

        public List<Type> GetTransactions()
        {
            return new List<Type> { typeof(Checkout) };
        }

        public TimeSpan? TimeLimit()
        {
            return null;
        }

        public List<int> GetNumberOfRequestsPerTransaction()
        {
            return new List<int> { 0, 0, 0 };
        }

        public List<TimeSpan> GetPeriodBetweenRequestsOfSameTransaction()
        {
            return new List<TimeSpan> { new TimeSpan(2000) };
        }

        List<string> IUseCaseConfig.GetTransactions()
        {
            throw new NotImplementedException();
        }

        public Distribution GetDistribution()
        {
            return Distribution.NORMAL;
        }

        public List<string> GetDistributionOfTransactions()
        {
            throw new NotImplementedException();
        }

        public Dictionary<string, string> GetUrlMap()
        {
            throw new NotImplementedException();
        }
    }
}
