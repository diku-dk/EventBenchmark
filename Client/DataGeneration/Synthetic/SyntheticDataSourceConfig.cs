namespace Client.DataGeneration
{
	public class SyntheticDataSourceConfig
    {

        public string connectionString { get; set; } //= "Data Source=file.db"; // "DataSource=:memory:"

        public int numCustomers { get; set; } = 10;

        public int numProducts { get; set; } = 50;

        public int avgNumProdPerSeller { get; set; } = 5;

        public bool createSchema { get; set; } = true;

    }
}

