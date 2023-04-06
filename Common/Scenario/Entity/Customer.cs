using System;
namespace Common.Scenario.Entity
{
	public class Customer
    {

        public long customer_id { get; set; }

        public string first_name { get; set; }

        public string last_name { get; set; }

        public string street { get; set; }

        public string complement { get; set; }

        public string customer_zip_code_prefix { get; set; }

        public string customer_city { get; set; }

        public string customer_state { get; set; }

        public string card_number { get; set; }

        public string card_security_number { get; set; }

        public string card_expiration { get; set; }

        public string card_holder_name { get; set; }

        public string card_type { get; set; }

        public int sucess_payment_count { get; set; }

        public int failed_payment_count { get; set; }

        public int delivery_count { get; set; }

        public int abandoned_cart_count { get; set; }

        public string data { get; set; }

    }
}

