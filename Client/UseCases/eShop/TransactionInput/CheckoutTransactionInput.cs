namespace Client.UseCases.eShop.TransactionInput
{
    public class CheckoutTransactionInput : IInput
    {

        public int MinNumItems { get; set; }
        public int MaxNumItems { get; set; }

        public int MinItemQty { get; set; }
        public int MaxItemQty { get; set; }

        public string CartUrl { get; set; }

    }
}
