﻿namespace Common.Entities
{
    public sealed class Product
    {
        public int seller_id { get; set; }

        public int product_id { get; set; }

        public string name { get; set; } = "";

        public string sku { get; set; } = "";

        public string category { get; set; } = "";

        public string description { get; set; } = "";

        public float price { get; set; }

        public float freight_value { get; set; }

        public string status { get; set; } = "approved";

        public string version { get; set; }

        public Product() { }

        public Product(Product product, string version)
        {
            this.seller_id = product.seller_id;
            this.product_id = product.product_id;
            this.name = product.name;
            this.sku = product.sku;
            this.category = product.category;
            this.description = product.description;
            this.price = product.price;
            this.freight_value = product.freight_value;
            this.status = product.status;
            this.version = version;
        }
  
        public Product(Product product, float newPrice)
        {
            this.seller_id = product.seller_id;
            this.product_id = product.product_id;
            this.name = product.name;
            this.sku = product.sku;
            this.category = product.category;
            this.description = product.description;
            this.price = newPrice;
            this.freight_value = product.freight_value;
            this.status = product.status;
            this.version = product.version;
        }

    }
}