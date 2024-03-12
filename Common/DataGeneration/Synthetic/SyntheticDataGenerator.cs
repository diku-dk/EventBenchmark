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

            Console.WriteLine("Seller, Product, and Stock Item generation in progress...");                        
            ConsoleUtility.WriteProgressBar(0);            
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
                currSellerId++;

                perc = (float)currProductId / this.config.numProducts;
                ConsoleUtility.WriteProgressBar((int)(perc * 100), true);                
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

            ConsoleUtility.WriteProgressBar(0);
            float perc;
            while (currCustomer <= this.config.numCustomers)
            {
                GenerateCustomer(command_, currCustomer);
                currCustomer++;
                perc = (float)currCustomer / this.config.numCustomers;
                ConsoleUtility.WriteProgressBar((int)(perc * 100), true);
            }
            Console.WriteLine();
        }
      
    }

}

