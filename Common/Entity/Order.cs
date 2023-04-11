using System;
namespace Common.Scenario.Entity
{
    /*
     * Order is based on two sources:
     * (i) Olist data set (kaggle)
     * (ii) Olist developer API: https://dev.olist.com/docs/retrieving-order-informations
     * The total attribute is also added to sum the value of all products in the order.
     */
    public class Order
	{
        // PK
        public long id { get; set; }

        // FK
        public long customer_id { get; set; }

        public string status { get; set; }

        public string purchase_timestamp { get; set; }

        // public string approved_at { get; set; }

        // added
        public string payment_date { get; set; }

        public string delivered_carrier_date { get; set; }

        public string delivered_customer_date { get; set; }

        public string estimated_delivery_date { get; set; }

        // dev
        public string count_items { get; set; }
        public string created_at { get; set; }
        public string updated_at { get; set; }

        public decimal total_amount { get; set; }
        public decimal total_freight { get; set; }
        public decimal total_incentive { get; set; }
        public decimal total_invoice { get; set; }
        public decimal total_items { get; set; }

        public Order()
        {
            this.status = OrderStatus.CREATED.ToString();
        }

    }
}

