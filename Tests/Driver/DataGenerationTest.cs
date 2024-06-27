using Common.DataGeneration;
using DuckDB.NET.Data;

namespace Tests.Driver;

public class DataGenerationTest
{
    private static readonly bool DISK = true;
    
    [Fact]
    public void TestDataGeneration()
    {
        // options: "Data Source=file.db"; // "DataSource=:memory:"
        DuckDBConnection connection;
        if (DISK)
        {
            connection = new DuckDBConnection("Data Source=file.db");
        } else
        {
            connection = new DuckDBConnection("DataSource=:memory:");
        }
        
        connection.Open();
        SyntheticDataSourceConfig previousData = new SyntheticDataSourceConfig()
        {
            numCustomers = 10000,
            numProducts = 10000,
            numProdPerSeller = 10,
            qtyPerProduct = 10000
        };
        var dataGen = new SyntheticDataGenerator(previousData);
        dataGen.CreateSchema(connection);
        dataGen.Generate(connection, true);

    }


}


