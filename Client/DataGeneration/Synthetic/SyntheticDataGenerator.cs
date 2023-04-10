using Client.Infra;
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
     * Based on Olist data model
     * Some  attribute values not found in olist are generated following TPC-C
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
            long numGeo = GetNumGeolocations(connection);

            // categories -- already loaded
            long numCat = GetNumCategories(connection);

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
                GenerateSeller(command, currSellerId, geo[0], geo[1], geo[2]);

                for(int i = 1; i < numProductsForSeller; i++)
                {
                    // get category
                    var category = GetCategory(command, numCat);
                    GenerateProduct(command, currProductId, currSellerId, category);
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

        private static long GetNumCategories(DuckDBConnection connection)
        {
            return DuckDbUtils.Count(connection, "categories");
        }

        private static long GetNumGeolocations(DuckDBConnection connection)
        {
            return DuckDbUtils.Count(connection, "geolocation");
        }

        private string[] GetGeolocation(DuckDbCommand command, long numGeo)
        {
            long pos = random.Next(1, (int) numGeo + 1);
            command.CommandText = "SELECT geolocation_city, geolocation_state, geolocation_zip_code_prefix from geolocation where rowid ="+pos+";";
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

        private string GetCategory(DuckDbCommand command, long numCat)
        {
            int pos = random.Next(1, (int) numCat + 1);
            command.CommandText = "SELECT product_category_name from categories where rowid =" + pos;
            var executeNonQuery = command.ExecuteNonQuery();
            var queryResult = command.ExecuteReader();
            queryResult.Read();
            return queryResult.GetString(0);
        }
      
    }

}

