using System;
using System.Collections.Generic;
using System.Linq;

namespace Client.DataGeneration
{
	public abstract class BaseDataGenerator
	{

        protected const string numbers = "0123456789";
        protected const string alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        protected readonly Random random = new Random();

        protected readonly Dictionary<string, string> mapTableToCreateStmt = new()
        {
            ["sellers"] = "CREATE OR REPLACE TABLE sellers (seller_id INTEGER, name VARCHAR, street1 VARCHAR, street2 VARCHAR, seller_zip_code_prefix VARCHAR, seller_city VARCHAR, seller_state VARCHAR, tax REAL, ytd INTEGER, order_count INTEGER);",
            ["products"] = "CREATE OR REPLACE TABLE products (product_id INTEGER, seller_id INTEGER, name VARCHAR, product_category_name VARCHAR, price REAL, data VARCHAR);",
            ["stock_items"] = "CREATE OR REPLACE TABLE stock_items (product_id INTEGER, seller_id INTEGER, quantity INTEGER, order_count INTEGER, ytd INTEGER, data VARCHAR);",
            ["customers"] = "CREATE OR REPLACE TABLE customers (customer_id INTEGER, name VARCHAR, last_name VARCHAR, street1 VARCHAR, street2 VARCHAR, " +
                            "customer_zip_code_prefix VARCHAR, customer_city VARCHAR, customer_state VARCHAR, " +
                            "card_number VARCHAR, card_expiration VARCHAR, card_holder_name VARCHAR, card_type VARCHAR, " +
                            "sucess_payment_count INTEGER, failed_payment_count INTEGER, delivery_count INTEGER, abandoned_cart_count INTEGER, data VARCHAR);"
        };

        protected readonly string baseSellerQuery = "INSERT INTO sellers(seller_id, name, street1, street2, tax, ytd, order_count, seller_zip_code_prefix, seller_city, seller_state) VALUES ";

        protected readonly string baseProductQuery = "INSERT INTO products (product_id,seller_id,product_category_name,name,price,data) VALUES ";

        public abstract void Generate();

        protected static string RemoveBadCharacter(string str) {
            if (str.Contains('\''))
            {
                return str.Replace("'", "");
            }
            return str;
        }

        protected string RandomString(int length, string chars)
        {
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        protected int numeric(int m, bool signed)
        {
            var num = random.Next((int)Math.Pow(10, m), (int)Math.Pow(10, m + 1));
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 1) return -num;
            else return num;
        }

        protected float numeric(int m, int n, bool signed)
        {
            float the_number;
            var str = RandomString(m, numbers);
            if (m == n) the_number = float.Parse("0." + str);
            else if (m > n)
            {
                var left = str.Substring(0, m - n);
                var right = str.Substring(m - n);
                the_number = float.Parse(left + "." + right);
            }
            else
            {
                var left = "0.";
                for (int i = 0; i < n - m; i++) left += "0";
                the_number = float.Parse(left + str);
            }
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 0) return -the_number;
            return the_number;
        }

    }
}

