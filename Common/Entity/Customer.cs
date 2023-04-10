using System;
namespace Common.Scenario.Entity
{
    /**
     * 
     */
	public class Customer
    {
        // olist data set
        public long id { get; set; }

        // added
        public string name { get; set; }

        public string address { get; set; }

        public string complement { get; set; }

        // olist data set
        public string zip_code_prefix { get; set; }

        public string city { get; set; }

        public string state { get; set; }

        // card
        public string card_number { get; set; }

        public string card_security_number { get; set; }

        public string card_expiration { get; set; }

        public string card_holder_name { get; set; }

        public string card_type { get; set; }

        // statistics
        public int success_payment_count { get; set; }

        public int failed_payment_count { get; set; }

        public int delivery_count { get; set; }

        public int abandoned_cart_count { get; set; }

        // additional
        public string data { get; set; }

    }
}

