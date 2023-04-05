using System;
namespace Common.Scenario.Entity
{
	public class OrderPaymentCard
	{
        // FKs
        public long order_id { get; set; }
        public int payment_sequential { get; set; }

        // card info coming from customer checkout
        public string card_number { get; set; }

        public string card_holder_name { get; set; }

        public string card_expiration { get; set; }

        // public string card_security_number { get; set; }

        public string card_brand { get; set; }
    }
}

