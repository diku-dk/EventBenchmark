using System;
namespace Common.Scenario.Customer
{
    public enum CustomerWorkerStatus
    {
        IDLE,
        BROWSING,
        CHECKOUT_SENT,
        CHECKOUT_NOT_SENT,
        CHECKOUT_FAILED,
        REACT_OUT_OF_STOCK,
        REACT_FAILED_PAYMENT,
        REACT_ABANDONED_CART
    }
}

