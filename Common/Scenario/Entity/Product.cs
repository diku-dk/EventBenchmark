using System;
namespace Common.Scenario.Entity
{
	public class Product
	{

        public long product_id { get; set; }

        public long seller_id { get; set; }

        public string name { get; set; }

        public string product_category_name { get; set; }

        public string data { get; set; }

        public decimal price { get; set; }
    }
}

