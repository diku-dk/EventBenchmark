namespace Common.Workload
{
    public enum TransactionType
    {
        // create a customer session
        CUSTOMER_SESSION, // end up with a checkout, abandoned cart ~~> out of stock, payment rejected
        // seller operations
        SELLER_SESSION, // price update + stock increase
        PRICE_UPDATE,
        DELETE_PRODUCT,
        // external service operation
        UPDATE_DELIVERY
    }
}
