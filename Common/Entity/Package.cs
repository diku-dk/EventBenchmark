using System;
namespace Common.Entity
{
	public class Package
	{
		// PK
		public long shipment_id;
		public int package_id;

		public decimal freight_value;

		// date the shipment has actually been performed
		public long shipping_date;

        // delivery date
        public long delivery_date;
		// public long estimated_delivery_date;

		// delivery to carrier date
		// seller must deliver to carrier
		// public long delivered_carrier_date;

        // product identification
        public long seller_id;
        public long product_id;
		public int quantity;

		public string status { get; set; }
    }
}

