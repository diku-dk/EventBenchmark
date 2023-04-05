using System;
namespace Common.Scenario.Entity
{
	public class OrderPayment
	{
        public long order_id { get; set; }

        public int payment_sequential { get; set; }

        public string payment_type { get; set; }

        public int payment_installments { get; set; }

        public decimal payment_value { get; set; }
    }
}

