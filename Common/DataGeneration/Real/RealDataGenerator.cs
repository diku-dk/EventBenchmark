using DuckDB.NET.Data;
using System;
using System.IO;
using System.Text;

namespace Common.DataGeneration.Real
{
    public sealed class RealDataGenerator : BaseDataGenerator
    {
        private readonly OlistDataSourceConfiguration config;

        public RealDataGenerator(OlistDataSourceConfiguration config)
        {
            this.config = config;
        }

        public override void Generate(DuckDBConnection connection, bool genCustomer = false)
        {

            // make sure all files exist first
            foreach (var entry in config.mapTableToFileName)
            {
                if (!File.Exists(config.fileDir + "/" + entry.Value))
                {
                    throw new Exception("Cannot generate table \'" + entry.Key + "\'. File \'"+entry.Value+"\' cannot be found in "+ config.fileDir);
                }
            }

            var command = connection.CreateCommand();
            var sb = new StringBuilder();
            foreach (var entry in config.mapTableToFileName)
            {
                sb.Append("CREATE OR REPLACE TABLE ").Append(entry.Key).Append("_aux").Append(" AS SELECT * FROM read_csv('")
                    .Append(config.fileDir).Append('/').Append(entry.Value)
                    .Append("', header=true, delim=',', AUTO_DETECT=TRUE);");

                command.CommandText = sb.ToString();
                var executeNonQuery = command.ExecuteNonQuery();
                sb.Clear();
            }

            foreach (var entry in mapTableToCreateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }

            // apparently duckdb creates the seller_id_idx automatically from the csv load
            // "CREATE UNIQUE INDEX seller_id_idx ON sellers_aux(seller_id);"

            command.CommandText = "CREATE INDEX seller_id_idx ON order_items_aux(seller_id);";
            command.ExecuteNonQuery();

            command.CommandText = "CREATE UNIQUE INDEX product_id_idx ON products_aux(product_id);";
            command.ExecuteNonQuery();

            // throw the original data in the respective driver-managed tables

            // sellers
            LoadSellers(connection);

            // products and respective stock item
            LoadProducts(connection);

            // customers
            if(genCustomer)
                LoadCustomers(connection);

            command.CommandText = "ALTER TABLE categories_aux RENAME TO categories;";
            command.ExecuteNonQuery();

            Console.WriteLine("Olist data generation has finished.");
        }

        // use rowid for referring to customers and link to orders table
        private void LoadCustomers(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = "select c.rowid as customer_id, customer_city, customer_state, customer_zip_code_prefix from customers_aux;";
            var queryResult = command.ExecuteReader();
            string[] geo = new string[3];

            while (queryResult.Read())
            {
                var customerId = (int)queryResult.GetInt64(0);
                geo[0] = RemoveBadCharacter(queryResult.GetString(1));
                geo[1] = queryResult.GetString(2);
                geo[2] = queryResult.GetString(2);

                GenerateCustomer(command, customerId, new Geolocation(geo[0], geo[1], geo[2]));

            }
        }

        // generate stock table based on products
        
        /**
         * 
         * TODO Make sure the product IDs are monotonically increasing for every seller
         */
        private void LoadProducts(DuckDBConnection connection)
        {
            // seller is not found in products table
            // order items provide the relationship between seller and product

            var command = connection.CreateCommand();
            command.CommandText = "create table seller_products AS select s.rowid as seller_id, o.product_id, o.price from order_items_aux as o inner join sellers_aux as s on o.seller_id = s.seller_id group by s.rowid, o.product_id, o.price;";
            command.ExecuteReader();

            // get product data
            command.CommandText = "select p.rowid as product_id, sp.seller_id, p.product_category_name from seller_products as sp inner join products_aux as p on sp.product_id = p.product_id;";
            var queryResult = command.ExecuteReader();

            while (queryResult.Read())
            {
                var productId = (int)queryResult.GetInt64(0);
                var sellerId = (int)queryResult.GetInt64(1);
                var category = queryResult.GetString(2);
                GenerateProduct(command, productId, sellerId, category);
                GenerateStockItem(command, productId, sellerId);
            }

        }

        private void LoadSellers(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = "select rowid, * from sellers_aux;";
            var queryResult = command.ExecuteReader();

            while (queryResult.Read())
            {
                int sellerID = queryResult.GetInt32(0);
                
                // get from original tuple
                string zip = queryResult.GetString(2);
                string city = RemoveBadCharacter( queryResult.GetString(3) );
                string state = queryResult.GetString(4);
                var geolocation = new Geolocation(city, state, zip);

                GenerateSeller(command, sellerID, geolocation);
            }

        }
    }

}
