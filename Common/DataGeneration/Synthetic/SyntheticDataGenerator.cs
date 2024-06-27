using Common.Infra;
using DuckDB.NET.Data;

namespace Common.DataGeneration
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
            foreach (var entry in this.mapTableToCreateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }
        }

        public void TruncateTables(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            foreach (var entry in this.mapTableToTruncateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }
        }

        public override void Generate(DuckDBConnection connection, bool genCustomer = false)
        {
            Console.WriteLine("Synthetic data generation started.");

            // products, stock, and link to respective sellers
            int remainingProducts = this.config.numProducts;

            int currSellerId = 1;
            int currProductId = 1;
            int numProductsForSeller;

            var command = connection.CreateCommand();

            Console.WriteLine("Seller and respective Product and Stock Item records generation in progress...");          
            float perc;
            while (remainingProducts > 0)
            {
                numProductsForSeller = Math.Min(this.config.numProdPerSeller, remainingProducts);

                // create seller                
                GenerateSeller(command, currSellerId);                
                for(int i = 1; i <= numProductsForSeller; i++)
                {
                    GenerateProduct(command, i, currSellerId);                    
                    GenerateStockItem(command, i, currSellerId, config.qtyPerProduct);                
                    currProductId++;
                }                
                remainingProducts -= numProductsForSeller;
                perc = (float)currProductId / this.config.numProducts;
                Console.WriteLine(numProductsForSeller+" products created for seller "+currSellerId+". %: "+perc*100);
                currSellerId++;
            }

            Console.WriteLine();

            // customers
            if (genCustomer)
            {
                GenerateCustomers(connection, command);
            }

            Console.WriteLine();
            Console.WriteLine("Synthetic data generation has terminated.");

        }

        public void GenerateCustomers(DuckDBConnection connection, DuckDbCommand command = null)
        {
            Console.WriteLine("Customer generation in progress...");
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
            float perc;
            while (currCustomer <= this.config.numCustomers)
            {
                GenerateCustomer(command_, currCustomer);
                perc = (float)currCustomer / this.config.numCustomers;
                Console.WriteLine("Customer "+currCustomer+" created. Total %: "+perc*100);
                currCustomer++;
            }
        }
      
    }

}

