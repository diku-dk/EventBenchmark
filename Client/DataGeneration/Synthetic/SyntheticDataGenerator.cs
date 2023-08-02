using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;

namespace Client.DataGeneration
{
    /**
     * Based on Olist data model
     * Some  attribute values not found in olist are generated following TPC-C
     */
    public class SyntheticDataGenerator : BaseDataGenerator
    {

        private readonly SyntheticDataSourceConfig config;

        public SyntheticDataGenerator(SyntheticDataSourceConfig config) : base()
        {
            this.config = config;
        }

        /**
         * Create tables
         */
        public void CreateSchema(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            // add remaining tables
            foreach (var entry in mapTableToCreateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }
        }

        public void TruncateTables(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            foreach (var entry in mapTableToTruncateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }
        }

        public void GenerateCustomers(DuckDBConnection connection, DuckDbCommand command = null)
        {
            DuckDbCommand command_;
            if (command is null)
            {
                command_ = connection.CreateCommand();
            }
            else
            {
                command_ = command;
            }
            int currCustomer = 1;
            while (currCustomer <= config.numCustomers)
            {
                GenerateCustomer(command_, currCustomer);
                currCustomer++;
            }
        }

        public override void Generate(DuckDBConnection connection, bool genCustomer = false)
        {
            logger.LogInformation("Synthetic data generation started.");

            // products, stock, and link to respective sellers
            int remainingProducts = config.numProducts;

            int currSellerId = 1;
            int currProductId = 1;
            int numProductsForSeller;

            var command = connection.CreateCommand();

            while (remainingProducts > 0)
            {
                numProductsForSeller = Math.Min(config.numProdPerSeller, remainingProducts);

                // create seller
                GenerateSeller(command, currSellerId);

                for(int i = 1; i <= numProductsForSeller; i++)
                {
                    GenerateProduct(command, currProductId, currSellerId);
                    GenerateStockItem(command, currProductId, currSellerId);
                    currProductId++;
                }

                remainingProducts -= numProductsForSeller;
                currSellerId++;
            }

            // customers
            if (genCustomer)
            {
                GenerateCustomers(connection, command);
            }

            logger.LogInformation("Synthetic data generation has terminated.");

        }
      
    }

}

