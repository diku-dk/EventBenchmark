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

        public bool createSchema = true;

        public string fileDir = Environment.GetEnvironmentVariable("HOME") + "/workspace/EventBenchmark/Client/DataGeneration/Synthetic";

        public Dictionary<string, string> mapTableToFileName = new()
        {
            ["categories"] = "product_category_name_translation.csv",
            ["geolocation"] = "olist_geolocation_dataset.csv"
        };

    }
}

