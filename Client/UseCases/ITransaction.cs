using System.Threading.Tasks;

namespace Client.UseCases.eShop
{
    public interface ITransaction
    {

        // private partial Task doSomething(NumberGenerator numberGenerator, PriceUpdateTransactionInput input, TimeSpan timeSpan) { }

        Task Run();


        Task Close()
        {
            // do nothing by default
            return Task.CompletedTask;
        }

    }
}
