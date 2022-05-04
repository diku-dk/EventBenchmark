using System.Threading.Tasks;

namespace Client.UseCases.eShop
{
    public interface ITransaction
    {

        Task Run();


        Task Close()
        {
            // do nothing by default
            return Task.CompletedTask;
        }

    }
}
