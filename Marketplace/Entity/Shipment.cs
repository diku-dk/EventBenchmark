using System;
namespace Marketplace.Entity
{
	public class Shipment
	{
		// PK
		public long shipment_id;
        public long order_id;
        public long customer_id;

		// materialized aggregate values from packages
		public int package_count;
		public decimal total_freight_value;

		// date all deliveries were requested
        public string request_date;

        public string status;

        // customer delivery address. the same for all packages/sellers
        public string first_name { get; set; }

        public string last_name { get; set; }

        public string street { get; set; }

        public string complement { get; set; }

        public string customer_zip_code_prefix { get; set; }

        public string customer_city { get; set; }

        public string customer_state { get; set; }
    }
}

