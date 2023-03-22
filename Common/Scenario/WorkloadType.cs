namespace Common.Scenario
{
    public enum WorkloadType
    {
        // create a customer session
        CUSTOMER_SESSION, // end up with a checkout, abandoned cart ~~> out of stock, payment rejected
        // seller operations
        PRICE_UPDATE,
        // STOCK_REPLENISHMENT,
        DELETE_PRODUCT,
        // external service operation
        UPDATE_DELIVERY

    }
}
