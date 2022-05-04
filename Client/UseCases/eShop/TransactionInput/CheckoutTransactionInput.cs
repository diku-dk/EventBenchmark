namespace Client.UseCases.eShop.TransactionInput
{
    public class CheckoutTransactionInput
    {

        public int MinNumItems { get; set; }
        public int MaxNumItems { get; set; }

        public int MinItemQty { get; set; }
        public int MaxItemQty { get; set; }

        public string CartUrl { get; set; }

        public string UserId { get; set; }

    }
}
