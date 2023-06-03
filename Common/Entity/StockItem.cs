using System;
namespace Common.Entity
{
    public class StockItem
    {
        public long product_id { get; set; }

        public long seller_id { get; set; }

        public int qty_available { get; set; }

        public int qty_reserved { get; set; }

        public int order_count { get; set; }

        public int ytd { get; set; }

        public bool active { get; set; }

        public DateTime created_at { get; set; }

        public DateTime updated_at { get; set; }

        public string data { get; set; } = "";
    }
}