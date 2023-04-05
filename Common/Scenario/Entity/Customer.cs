using System;
namespace Common.Scenario.Entity
{
	public class Customer
    {

        public long customer_id { get; set; }

        public string first_name { get; set; }

        public string last_name { get; set; }

        public string street1 { get; set; }

        public string street2 { get; set; }

        public string customer_zip_code_prefix { get; set; }

        public string customer_city { get; set; }

        public string customer_state { get; set; }

        public string card_number { get; set; }

        public string card_security_number { get; set; }

        public string card_expiration { get; set; }

        public string card_holder_name { get; set; }

        public string card_type { get; set; }

        public string sucess_payment_count { get; set; }

        public string failed_payment_count { get; set; }

        public string delivery_count { get; set; }

        public string abandoned_cart_count { get; set; }

        public string data { get; set; }

    }
}

