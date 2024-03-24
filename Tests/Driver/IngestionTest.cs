using Common.DataGeneration;
using Common.Ingestion;
using Common.Ingestion.Config;
using DuckDB.NET.Data;

namespace Tests.Driver;

public class IngestionTest
{
	[Fact]
	public async void TestIngestion()
	{

        if(OperatingSystem.IsWindows())
        {
            Console.WriteLine("DuckDB library does not work in Windows platform. The test will not run.");
            Assert.True(true);
            return;
        }

		var connection = new DuckDBConnection("DataSource=:memory:");

        connection.Open();

		SyntheticDataSourceConfig dataSourceConfig = new SyntheticDataSourceConfig()
        {
            numCustomers = 10,
            numProducts = 10,
            numProdPerSeller = 2
        };

        var dataGen = new SyntheticDataGenerator(dataSourceConfig);
        dataGen.CreateSchema(connection);
        // dont need to generate customers on every run. only once
        // dataGen.GenerateCustomers(connection);
        dataGen.Generate(connection, true);

        IngestionConfig ingestionConfig = new IngestionConfig()
        {
            connectionString = "DataSource=:memory:",
            strategy = IngestionStrategy.WORKER_PER_CPU,
            concurrencyLevel = 4,
            mapTableToUrl = new Dictionary<string, string>()
            {
                //{ "sellers" , "http://localhost:8080/seller" },
                //{ "customers" , "http://localhost:8080/customer" },
                //{ "stock_items", "http://localhost:8080/stock" },
                { "products", "http://localhost:8080/product"}
            }
        };

        await IngestionOrchestrator.Run(connection, ingestionConfig);

        // retrieve some random and see if they match

	}
}

