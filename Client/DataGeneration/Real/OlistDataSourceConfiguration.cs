using System;
using System.Collections.Generic;
using System.IO;
using Orleans;

namespace Client.DataGeneration.Real
{
	public class OlistDataSourceConfiguration
    {
        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        // only set if non local
        public string fileDir = Environment.GetEnvironmentVariable("HOME") + "/Downloads/olist/";

        // created synthetically
        public int percentageFailedOrders = 10;

        public Dictionary<string, string> mapTableToFileName = new()
        {
            ["categories"] = "product_category_name_translation.csv",
            ["sellers"] = "olist_sellers_dataset.csv",
            ["products"] = "olist_products_dataset.csv",
            ["customers"] = "olist_customers_dataset.csv",
            ["orders"] = "olist_orders_dataset.csv",
            ["order_items"] = "olist_order_items_dataset.csv"
        };

    }
}

