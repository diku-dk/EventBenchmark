using System;
namespace Common.Scenario.Entity
{
    /**
     * https://dev.olist.com/docs/orders
     */
    public enum OrderStatus
	{
        CREATED,
        /***
         * Given the separation of Marketplace and olist 
         * (https://dev.olist.com/docs/orders-notifications-details),
         * the status "processing" is used to flag the invoice
         * is ought to be emmitted from the seller.
         * In our case, we ignore this status because the payment
         * is being handled directly by our application as in
         * most e-commerce systems. In sum, we don't use this status
         */
        PROCESSING,
        CANCELED,
        UNAVAILABLE,
        INVOICED,
        SHIPPED,
        DELIVERED,


        // created for the benchmark
        PAYMENT_FAILED
    }
}

