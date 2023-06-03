using System;
namespace Common.Entity
{
    public class OrderItem
    {
        public long order_id { get; set; }

        public long order_item_id { get; set; }

        public long product_id { get; set; }

        public string product_name { get; set; } = "";

        public long seller_id { get; set; }

        // prices change over time
        public decimal unit_price { get; set; }

        public DateTime shipping_limit_date { get; set; }

        public decimal freight_value { get; set; }

        // not present in olist
        public int quantity { get; set; }

        // without freight value
        public decimal total_items { get; set; }

        // without freight value
        public decimal total_amount { get; set; }

        //
        public decimal[] vouchers { get; set; } = Array.Empty<decimal>();

    }
}