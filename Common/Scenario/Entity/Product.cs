using System;
namespace Common.Scenario.Entity
{
    /**
     * Product requirements:
     * https://dev.olist.com/docs/creating-a-product
     * package mesasures and photo, tags, attributes are not considered
     * Only one category is chosen as found in olist public data set
     */
    public class Product
	{
        public long product_id { get; set; }

        public long seller_id { get; set; }

        public string name { get; set; }

        public string sku { get; set; }

        public string product_category_name { get; set; }

        public string description { get; set; }

        public decimal price { get; set; }

        // "2017-10-06T01:40:58.172415Z"
        public string updated_at { get; set; }

        public bool active { get; set; }

        // https://dev.olist.com/docs/products
        // approved by default
        public string status { get; set; }

        
    }
}

