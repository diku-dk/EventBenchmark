using System;
using System.Collections.Generic;
using System.IO;
using Common.Ingestion.Config;
using Orleans;

namespace Client.DataGeneration
{
	public class SyntheticDataSourceConfiguration
    {

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        public int numCustomers = 300;

        public int numProducts = 100;

        public int avgNumProdPerSeller = 5;

        // number of stock per product. a range min max
        // avg number of product per seller. a range.
        // categories. just load from the original

        public bool createSchema = true;

        // public readonly string homePath = Environment.GetEnvironmentVariable("HOME");

        public readonly string fileDir = Environment.GetEnvironmentVariable("HOME") + "/workspace/EventBenchmark/Client/DataGeneration/Synthetic";

        public readonly Dictionary<string, string> mapTableToFileName = new()
        {
            ["categories"] = "product_category_name_translation.csv",
            ["geolocation"] = "olist_geolocation_dataset.csv"
        };

        public readonly Dictionary<string, string> mapTableToCreateStmt = new()
        {

            ["sellers"] = "CREATE OR REPLACE TABLE sellers (seller_id INTEGER, name VARCHAR, street1 VARCHAR, street2 VARCHAR, seller_zip_code_prefix VARCHAR, seller_city VARCHAR, seller_state VARCHAR, tax REAL, ytd INTEGER, order_count INTEGER);",
            ["products"] = "CREATE OR REPLACE TABLE products (product_id INTEGER, seller_id INTEGER, name VARCHAR, product_category_name VARCHAR, price REAL, data VARCHAR);",
            ["stock_items"] = "CREATE OR REPLACE TABLE stock_items (product_id INTEGER, seller_id INTEGER, quantity INTEGER, order_count INTEGER, ytd INTEGER, data VARCHAR);",
            ["customers"] = "CREATE OR REPLACE TABLE customers (customer_id INTEGER, name VARCHAR, last_name VARCHAR, street1 VARCHAR, street2 VARCHAR, " +
                            "customer_zip_code_prefix VARCHAR, customer_city VARCHAR, customer_state VARCHAR, " +
                            "card_number VARCHAR, card_expiration VARCHAR, card_holder_name VARCHAR, card_type VARCHAR, " +
                            "sucess_payment_count INTEGER, failed_payment_count INTEGER, delivery_count INTEGER, abandoned_cart_count INTEGER, data VARCHAR);"
        };

    }
}

