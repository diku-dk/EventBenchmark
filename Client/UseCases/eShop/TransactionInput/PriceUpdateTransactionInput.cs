namespace Client.UseCases.eShop.TransactionInput
{
    public class PriceUpdateTransactionInput : IInput
    {

        public int NumTotalItems { get; set; }
        public string CatalogUrl { get; set; }

    }
}
