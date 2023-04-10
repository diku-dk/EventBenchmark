using System;
namespace Common.Scenario.Entity
{
    /**
	 * Based on the payload found in:
	 * https://dev.olist.com/docs/orders-grouping-by-shipping_limite_date
	 * "id": "2a44f8af-dbed-4f47-9a48-0832e3306194",
     *  "created_at": "2021-10-08T16:11:57.171099Z",
     *   "status": "ready_to_ship"
	 */
    public class OrderHistory
	{
		// PK. 
		public int id;
        // FK can be ommitted if document-oriented model, as a nested object
        public long order_id;

        public string created_at;
		public string status;

		public OrderHistory()
		{

		}

	}
}

