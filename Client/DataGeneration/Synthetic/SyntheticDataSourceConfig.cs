using System;
using System.Collections.Generic;
using System.IO;
using Orleans;

namespace Client.DataGeneration
{
	public class SyntheticDataSourceConfig
    {

        public string connectionString = "Data Source=file.db"; // "DataSource=:memory:"

        public int numCustomers = 10;

        public int numProducts = 50;

        public int avgNumProdPerSeller = 5;

        public bool createSchema = true;

    }
}

