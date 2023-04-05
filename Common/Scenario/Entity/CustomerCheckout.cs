using System;
namespace Common.Scenario.Entity
{
    /**
     * A sub-type of customer.
     * Ideally, address and credit card info may change across customer checkouts
     * Basket and Order does not need to know all internal data about customers
     */
    public class CustomerCheckout
    {
        public long CustomerId { get; set; }

        /**
         * Delivery
         */
        public string City { get; set; }

        public string Street { get; set; }

        public string State { get; set; }

        public string ZipCode { get; set; }

        public string PaymentType { get; set; }

        /**
         * Credit or debit card
         */
        public string CardNumber { get; set; }

        public string CardHolderName { get; set; }

        public string CardExpiration { get; set; }

        public string CardSecurityNumber { get; set; }

        public string CardBrand { get; set; }

        // if debit, must be null
        public int Installments { get; set; }

        // Vouchers to be applied
        public decimal[] Vouchers { get; set; }
    
    }
}

