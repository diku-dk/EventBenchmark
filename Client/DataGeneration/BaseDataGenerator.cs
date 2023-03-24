using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using System.Text;

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

        protected readonly string baseStockQuery = "INSERT INTO stock_items (product_id,seller_id,quantity,order_count,ytd,data) VALUES ";

        protected void GenerateStockItem(DuckDbCommand command, int productId, int sellerId)
        {
            var quantity = numeric(4, true);
            var ytd = numeric(2, false);
            var order_count = numeric(4, false);
            var data = RandomString(50, alphanumeric);

            // issue insert statement
            var sb = new StringBuilder(baseStockQuery);
            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerId).Append(',');
            sb.Append(quantity).Append(',');
            sb.Append(order_count).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append('\'').Append(data).Append("');");

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected readonly string baseCustomerQuery = "INSERT INTO customers (customer_id, name, last_name, street1, street2, " +
                    "customer_zip_code_prefix, customer_city, customer_state, " +
                    "card_number, card_expiration, card_holder_name, card_type, " +
                    "sucess_payment_count, failed_payment_count, delivery_count, abandoned_cart_count, data) VALUES ";

        protected Array cardValues = CardType.GetValues(typeof(CardType));

        protected void GenerateCustomer(DuckDbCommand command, int customerId, string[] geo)
        {
            var name = RandomString(16, alphanumeric);
            var lastName = RandomString(16, alphanumeric);
            var street1 = RandomString(20, alphanumeric);
            var street2 = RandomString(20, alphanumeric);

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
            if (random.Next(1, 11) < 8)//70% change
            {
                var middleName = RandomString(16, alphanumeric);
                cardHolderName = name + " " + middleName + " " + lastName;
            }
            else
            {
                cardHolderName = RandomString(16, alphanumeric);
            }
            var cardType = (string)cardValues.GetValue(random.Next(1, cardValues.Length)).ToString();

            var sucess_payment_count = numeric(4, false);
            var failed_payment_count = numeric(4, false);
            var C_DELIVERY_CNT = numeric(4, false);
            var C_DATA = RandomString(500, alphanumeric);

            var abandonedCartsNum = numeric(4, false);

            var sb = new StringBuilder(baseCustomerQuery);
            sb.Append('(').Append(customerId).Append(',');
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

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();

        }


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

