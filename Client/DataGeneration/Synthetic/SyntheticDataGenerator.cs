using Common.Ingestion.DTO;
using Common.Serdes;
using Confluent.Kafka;
using DuckDB.NET.Data;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics.Metrics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Client.DataGeneration
{
    /**
     * Based on TPC-C values
     */
    public class SyntheticDataGenerator : BaseDataGenerator
    {

        private readonly SyntheticDataSourceConfiguration config;

        public SyntheticDataGenerator(SyntheticDataSourceConfiguration config) : base()
        {
            this.config = config;
        }

        /**
         * Create tables
         */
        private void Prepare(DuckDBConnection connection)
        {

            foreach (var entry in config.mapTableToFileName)
            {
                if (!File.Exists(config.fileDir + "/" + entry.Value))
                {
                    throw new Exception("Cannot generate table " + entry.Key + ". File cannot be found.");
                }
            }

            var command = connection.CreateCommand();
            var sb = new StringBuilder();
            foreach (var entry in config.mapTableToFileName)
            {
                sb.Append("CREATE OR REPLACE TABLE ").Append(entry.Key).Append(" AS SELECT * FROM read_csv('")
                    .Append(config.fileDir).Append('/').Append(entry.Value)
                    .Append("', header=true, delim=',', AUTO_DETECT=TRUE);");

                command.CommandText = sb.ToString();
                command.ExecuteNonQuery();
                sb.Clear();
            }

            // add remaining tables
            foreach (var entry in mapTableToCreateStmt)
            {
                command.CommandText = entry.Value;
                command.ExecuteNonQuery();
            }

        }

        public override void Generate()
        {
            using var connection = new DuckDBConnection(config.connectionString);
            connection.Open();

            if (config.createSchema)
            {
                Prepare(connection);
            }

            // var command = connection.CreateCommand();
            // command.CommandText = "CREATE TEMP TABLE geo_aux AS (SELECT geolocation_state, geolocation_city, COUNT(*) as num FROM geolocation GROUP BY geolocation_state, geolocation_city)";

            // location of customers and sellers -- already loaded
            // get number of geolocations
            int numGeo = GetNumGeolocations(connection);

            // categories -- already loaded
            int numCat = GetNumCategories(connection);

            // products, stock, and link to respective sellers
            int remainingProducts = config.numProducts;

            int currSellerId = 1;
            int currProductId = 1;
            int numProductsForSeller = 0;

            var command = connection.CreateCommand();

            while (remainingProducts > 0)
            {
                numProductsForSeller = random.Next(1, config.avgNumProdPerSeller + 1);

                //
                numProductsForSeller = Math.Min(numProductsForSeller, remainingProducts);

                // create seller
                string[] geo = GetGeolocation(command, numGeo);
                GenerateSeller(command, currSellerId, geo);

                for(int i = 1; i < numProductsForSeller; i++)
                {
                    GenerateProduct(command, currProductId, currSellerId, numCat);
                    GenerateStockItem(command, currProductId, currSellerId);
                    currProductId++;
                }

                remainingProducts = remainingProducts - numProductsForSeller;
                currSellerId++;
            }

            // customers
            int currCustomer = 1;
            while(currCustomer <= config.numCustomers)
            {
                string[] geo = GetGeolocation(command, numGeo);
                GenerateCustomer(command, currCustomer, geo);
                currCustomer++;
            }

            Console.WriteLine("Synthetic data generation has finished");

        }

        private static int GetNumCategories(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) from categories;";
            var executeNonQuery = command.ExecuteNonQuery();
            var reader = command.ExecuteReader();
            reader.Read();
            return reader.GetInt32(0);
        }

        private static int GetNumGeolocations(DuckDBConnection connection)
        {
            var command = connection.CreateCommand();
            command.CommandText = "SELECT COUNT(*) from geolocation;";
            var executeNonQuery = command.ExecuteNonQuery();
            var reader = command.ExecuteReader();
            reader.Read();
            return reader.GetInt32(0);

        }

        private string[] GetGeolocation(DuckDbCommand command, int numGeo)
        {
            int pos = random.Next(1, numGeo + 1);
            command.CommandText = "SELECT geolocation_city, geolocation_state, geolocation_zip_code_prefix from geolocation where rowid ="+pos+";";
            var executeNonQuery = command.ExecuteNonQuery();
            DuckDBDataReader queryResult = command.ExecuteReader();
            queryResult.Read();
            string[] res = new string[3];
            for (int ordinal = 0; ordinal < queryResult.FieldCount; ordinal++)
            {
                res[ordinal] = queryResult.GetString(ordinal);
            }
            if (res[0].Contains('\'')) {
                res[0] = res[0].Replace("'", "");
            }
            return res;
        }
            
        private void GenerateSeller(DuckDbCommand command, int sellerId, string[] geo)
        {
            string name = RandomString(10, alphanumeric);
            string street1 = RandomString(20, alphanumeric);
            string street2 = RandomString(20, alphanumeric);

            string city = geo[0];
            string state = geo[1];
            string zip = geo[2];

            float tax = numeric(4, 4, false);
            int ytd = 0;

            var order_count = numeric(4, false);

            // issue insert statement
            var sb = new StringBuilder(baseSellerQuery);
            sb.Append('(').Append(sellerId).Append(',');
            sb.Append('\'').Append(name).Append("',");
            sb.Append('\'').Append(street1).Append("',");
            sb.Append('\'').Append(street2).Append("',");
            sb.Append(tax).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append(order_count).Append(',');
            sb.Append('\'').Append(zip).Append("',");
            sb.Append('\'').Append(city).Append("',");
            sb.Append('\'').Append(state).Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        private string GetCategory(DuckDbCommand command, int numCat)
        {
            int pos = random.Next(1, numCat + 1);
            command.CommandText = "SELECT product_category_name from categories where rowid =" + pos;
            var executeNonQuery = command.ExecuteNonQuery();
            var queryResult = command.ExecuteReader();
            queryResult.Read();
            return queryResult.GetString(0);
        }

        private void GenerateProduct(DuckDbCommand command, int productId, int sellerId, int numCat)
        {
            // get category
            var category = GetCategory(command, numCat);

            var name = RandomString(24, alphanumeric);
            var price = numeric(5, 2, false);
            var data = RandomString(50, alphanumeric);

            // issue insert statement
            var sb = new StringBuilder(baseProductQuery);
            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerId).Append(',');
            sb.Append('\'').Append(category).Append("',");
            sb.Append('\'').Append(name).Append("',");
            sb.Append(price).Append(',');
            sb.Append('\'').Append(data).Append("');");

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }
      
    }

}

