using System.Collections.Generic;

namespace Client.UseCases.eShop
{
    public interface IUseCase
    {

        List<ITransaction> GetTransactions();

        List<int> GetPercentageOfTransaction();

        long GetPeriod();

        int getNumber();


    }
}
