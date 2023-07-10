using DuckDB.NET.Data;
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

        public override void Generate(bool genCustomer = false)
        {
            using var connection = new DuckDBConnection(config.connectionString);
            connection.Open();

            if (config.createSchema)
            {
                Prepare(connection);
            }

            // products, stock, and link to respective sellers
            int remainingProducts = config.numProducts;

            int currSellerId = 1;
            int currProductId = 1;
            int numProductsForSeller = 0;

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

                remainingProducts = remainingProducts - numProductsForSeller;
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

            Console.WriteLine("Synthetic data generation has terminated.");

        }
      
    }

}

