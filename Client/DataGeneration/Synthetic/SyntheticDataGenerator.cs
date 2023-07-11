using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using System;

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
        private void Prepare(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            // add remaining tables
            foreach (var entry in mapTableToCreateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }
        }

        public override void Generate(DuckDBConnection connection, bool genCustomer = false)
        {
            if (config.createSchema)
            {
                Prepare(connection);
            }

            logger.LogInformation("Synthetic data generation started.");

            // products, stock, and link to respective sellers
            int remainingProducts = config.numProducts;

            int currSellerId = 1;
            int currProductId = 1;
            int numProductsForSeller;

            var command = connection.CreateCommand();

            while (remainingProducts > 0)
            {
                numProductsForSeller = this.random.Next(1, config.avgNumProdPerSeller + 1);

                //
                numProductsForSeller = Math.Min(numProductsForSeller, remainingProducts);

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
                int currCustomer = 1;
                while (currCustomer <= config.numCustomers)
                {
                    GenerateCustomer(command, currCustomer);
                    currCustomer++;
                }
            }

            logger.LogInformation("Synthetic data generation has terminated.");

        }
      
    }

}

