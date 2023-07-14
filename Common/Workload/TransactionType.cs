namespace Common.Workload
{
    public enum TransactionType
    {
        // create a customer session
        CUSTOMER_SESSION, // end up with a checkout, abandoned cart ~~> out of stock, payment rejected
        // seller operations
        DASHBOARD,
        PRICE_UPDATE,
        DELETE_PRODUCT, // could be a very rare tx
        // 
        UPDATE_DELIVERY,
        NONE
    }
}
