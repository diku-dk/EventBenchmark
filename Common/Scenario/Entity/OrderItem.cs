using System;
namespace Common.Scenario.Entity
{
	public class OrderItem
	{
        public long order_id { get; set; }

        public long order_item_id { get; set; }

        public long product_id { get; set; }

        public long seller_id { get; set; }

        public decimal price { get; set; }

        public string shipping_limit_date { get; set; }

        public decimal freight_value { get; set; }

        // not present in olist
        public int quantity { get; set; }
    }
}

