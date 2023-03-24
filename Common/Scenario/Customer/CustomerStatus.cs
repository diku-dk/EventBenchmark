using System;
namespace Common.Scenario.Customer
{
    public enum CustomerStatus
    {
        NEW,
        BROWSING,
        CHECKOUT_SENT,
        CHECKOUT_NOT_SENT,
        REACT_OUT_OF_STOCK,
        REACT_FAILED_PAYMENT,
        REACT_ABANDONED_CART
    }
}

