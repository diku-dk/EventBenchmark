using System;
namespace Common.Entity
{
	public class Shipment
	{
		// PK
		public long shipment_id;
        public long order_id;
        public long customer_id;

		// materialized values from packages
		public int package_count;
		public decimal total_freight_value;

		// date all deliveries were requested
        public string request_date;

        // shipment status
        public string status;

        // customer delivery address. the same for all packages/sellers
        public string first_name { get; set; }

        public string last_name { get; set; }

        public string street { get; set; }

        public string complement { get; set; }

        public string zip_code_prefix { get; set; }

        public string city { get; set; }

        public string state { get; set; }
    }
}

