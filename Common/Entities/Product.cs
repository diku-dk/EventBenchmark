namespace Common.Entities
{
    public class Product
    {

        public long seller_id { get; set; }

        public long product_id { get; set; }

        public string name { get; set; } = "";

        public string sku { get; set; } = "";

        public string category { get; set; } = "";

        public string description { get; set; } = "";

        public decimal price { get; set; }

        public decimal freight_value { get; set; }

        public string status { get; set; } = "approved";

        public bool active { get; set; }

    }
}