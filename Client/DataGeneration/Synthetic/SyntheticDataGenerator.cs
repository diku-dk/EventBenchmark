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

            int currSellerID = 1;
            int currProductID = 1;
            int numProductsForSeller = 0;

            while (remainingProducts > 0)
            {
                numProductsForSeller = random.Next(1, config.avgNumProdPerSeller + 1);

                //
                numProductsForSeller = Math.Min(numProductsForSeller, remainingProducts);

                // create seller
                GenerateSeller(connection, currSellerID, numGeo);

                for(int i = 1; i < numProductsForSeller; i++)
                {
                    GenerateProduct(connection, currProductID, currSellerID, numCat);
                    GenerateStockItem(connection, currProductID, currSellerID);
                    currProductID++;
                }

                remainingProducts = remainingProducts - numProductsForSeller;
                currSellerID++;
            }

            // customers
            int currCustomer = 1;
            while(currCustomer <= config.numCustomers)
            {
                GenerateCustomer(connection, currCustomer, numGeo);
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

        private string[] GetGeolocation(DuckDBConnection connection, int numGeo)
        {
            int pos = random.Next(1, numGeo + 1);
            var command = connection.CreateCommand();
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
            
        private void GenerateSeller(DuckDBConnection connection, int sellerId, int numGeo)
        {
            string name = RandomString(10, alphanumeric);
            string street1 = RandomString(20, alphanumeric);
            string street2 = RandomString(20, alphanumeric);

            // get from geolocation
            string[] geo = GetGeolocation(connection, numGeo);

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

            var command = connection.CreateCommand();

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        private string GetCategory(DuckDBConnection connection, int numCat)
        {
            int pos = random.Next(1, numCat + 1);
            var command = connection.CreateCommand();
            command.CommandText = "SELECT product_category_name from categories where rowid =" + pos;
            var executeNonQuery = command.ExecuteNonQuery();
            var queryResult = command.ExecuteReader();
            queryResult.Read();
            return queryResult.GetString(0);
        }

        private void GenerateProduct(DuckDBConnection connection, int productId, int sellerId, int numCat)
        {
            // get category
            var category = GetCategory(connection, numCat);

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

            var command = connection.CreateCommand();
            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        private readonly string baseStockQuery = "INSERT INTO stock_items (product_id,seller_id,quantity,order_count,ytd,data) VALUES ";

        private void GenerateStockItem(DuckDBConnection connection, int productId, int sellerID)
        {
            var quantity = numeric(4, true);
            var ytd = numeric(2, false);
            var order_count = numeric(4, false);
            var data = RandomString(50, alphanumeric);

            // issue insert statement
            var sb = new StringBuilder(baseStockQuery);
            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerID).Append(',');
            sb.Append(quantity).Append(',');
            sb.Append(order_count).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append('\'').Append(data).Append("');");

            var command = connection.CreateCommand();
            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        private readonly string baseCustomerQuery = "INSERT INTO customers (customer_id, name, last_name, street1, street2, " +
                            "customer_zip_code_prefix, customer_city, customer_state, " +
                            "card_number, card_expiration, card_holder_name, card_type, " +
                            "sucess_payment_count, failed_payment_count, delivery_count, abandoned_cart_count, data) VALUES ";

        private Array cardValues = CardType.GetValues(typeof(CardType));

        private void GenerateCustomer(DuckDBConnection connection, int customerID, int numGeo)
        {
            var name = RandomString(16, alphanumeric);
            var lastName = RandomString(16, alphanumeric);
            var street1 = RandomString(20, alphanumeric);
            var street2 = RandomString(20, alphanumeric);

            // get from geolocation
            string[] geo = GetGeolocation(connection, numGeo);
            var city = geo[0];
            var state = geo[1];
            var zip = geo[2];

            /*
            var C_PHONE = RandomString(16, alphanumeric);
            var C_SINCE = DateTime.Now;
            var C_CREDIT = RandomString(2, alphanumeric);
            var C_CREDIT_LIM = numeric(12, 2, true);
            var C_DISCOUNT = numeric(4, 4, true);
            var C_BALANCE = numeric(12, 2, true);
            var C_YTD_PAYMENT = numeric(12, 2, true);
            */
            var cardNumber = RandomString(16, numbers);
            var cardExpiration = RandomString(4, numbers);
            string cardHolderName = null;
            if (random.Next(1, 11) < 8) { //70% change
                var middleName = RandomString(16, alphanumeric);
                cardHolderName = name + " "+ middleName + " " + lastName;
            } else {
                cardHolderName = RandomString(16, alphanumeric);
            }
            var cardType = (string)cardValues.GetValue(random.Next(1, cardValues.Length)).ToString();

            var sucess_payment_count = numeric(4, false);
            var failed_payment_count = numeric(4, false);
            var C_DELIVERY_CNT = numeric(4, false);
            var C_DATA = RandomString(500, alphanumeric);

            var abandonedCartsNum = numeric(4, false);

            var sb = new StringBuilder(baseCustomerQuery);
            sb.Append('(').Append(customerID).Append(',');
            sb.Append('\'').Append(name).Append("',");
            sb.Append('\'').Append(lastName).Append("',");
            sb.Append('\'').Append(street1).Append("',");
            sb.Append('\'').Append(street2).Append("',");
            sb.Append('\'').Append(zip).Append("',");
            sb.Append('\'').Append(city).Append("',");
            sb.Append('\'').Append(state).Append("',");
            sb.Append('\'').Append(cardNumber).Append("',");
            sb.Append('\'').Append(cardExpiration).Append("',");
            sb.Append('\'').Append(cardHolderName).Append("',");
            sb.Append('\'').Append(cardType).Append("',");
            sb.Append(sucess_payment_count).Append(',');
            sb.Append(failed_payment_count).Append(',');
            sb.Append(C_DELIVERY_CNT).Append(',');
            sb.Append(abandonedCartsNum).Append(',');
            sb.Append('\'').Append(C_DATA).Append("');");

            Console.WriteLine(sb.ToString());

            var command = connection.CreateCommand();
            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();

        }

    }

}

