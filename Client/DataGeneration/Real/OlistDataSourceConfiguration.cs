using System;
using System.Collections.Generic;
using System.IO;
using Common.Ingestion.Config;
using Orleans;

namespace Client.DataGeneration.Real
{
	public class OlistDataSourceConfiguration
    {
        // public DataSourceType dataSourceType;

        // whether files are located in the path of the source code
        // public bool local;

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        // only set if non local
        public string filePath = Directory.GetCurrentDirectory();

        public Dictionary<string, string> mapTableToFileName = new()
        {
            ["categories"] = "product_category_name_translation.csv",
            ["sellers"] = "olist_sellers_dataset.csv",
            ["products"] = "olist_products_dataset.csv",
            ["customers"] = "olist_customers_dataset.csv",
            ["orders"] = "",
            ["order_items"] = ""
        };

    }
}

