using System;
namespace Common.Scenario.Entity
{
	public class Order
	{
        public long order_id { get; set; }

        public long customer_id { get; set; }

        public string order_status { get; set; }

        public string order_purchase_timestamp { get; set; }

        public string order_approved_at { get; set; }

        public string order_delivered_carrier_date { get; set; }

        public string order_delivered_customer_date { get; set; }

        public string order_estimated_delivery_date { get; set; }

        // not found in olist
        public decimal total { get; set; }

        public Order()
        {
            this.order_status = OrderStatus.CREATED.ToString();
        }

    }
}

