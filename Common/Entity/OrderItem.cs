using System;
namespace Common.Entity
{
	public class OrderItem
	{
        // Composite PK
        public long order_id { get; set; }
        public long order_item_id { get; set; }

        // FK
        public long product_id { get; set; }

        // FK
        public long seller_id { get; set; }

        public decimal unit_price { get; set; }

        public string shipping_limit_date { get; set; }

        public decimal freight_value { get; set; }

        // not present in olist
        public int quantity { get; set; }

        // without freight value
        public decimal total_items { get; set; }

        // with freight value
        public decimal total_amount { get; set; }
    }
}

