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
        protected const string alpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        protected const string alphanumericupper = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        protected const string alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        protected readonly Random random = new Random();

        protected static DateTime minDate = new DateTime(1940, 1, 1);
        protected static DateTime maxDate = new DateTime(2008, 12, 31);
        protected readonly long baseTicks = maxDate.Ticks - minDate.Ticks;

        protected readonly Dictionary<string, string> mapTableToCreateStmt = new()
        {
            ["sellers"] = "CREATE OR REPLACE TABLE sellers (id INTEGER, name VARCHAR, company_name VARCHAR, email VARCHAR, phone VARCHAR, mobile_phone VARCHAR, cpf VARCHAR, cnpj VARCHAR, address VARCHAR, complement VARCHAR, city VARCHAR, state VARCHAR, zip_code_prefix VARCHAR, order_count INTEGER);",
            ["products"] = "CREATE OR REPLACE TABLE products (id INTEGER, seller_id INTEGER, name VARCHAR, sku VARCHAR, category_name VARCHAR, description VARCHAR, price REAL, updated_at VARCHAR, active BOOLEAN, status VARCHAR);",
            ["stock_items"] = "CREATE OR REPLACE TABLE stock_items (product_id INTEGER, seller_id INTEGER, qty_available INTEGER, qty_reserved INTEGER, order_count INTEGER, ytd INTEGER, data VARCHAR);",
            ["customers"] = "CREATE OR REPLACE TABLE customers (id INTEGER, first_name VARCHAR, last_name VARCHAR, address VARCHAR, complement VARCHAR, birth_date VARCHAR, " +
                            "zip_code_prefix VARCHAR, city VARCHAR, state VARCHAR, " +
                            "card_number VARCHAR, card_security_number VARCHAR, card_expiration VARCHAR, card_holder_name VARCHAR, card_type VARCHAR, " +
                            "success_payment_count INTEGER, failed_payment_count INTEGER, delivery_count INTEGER, abandoned_cart_count INTEGER, data VARCHAR);"
        };

        protected readonly string baseSellerQuery = "INSERT INTO sellers(id, name, company_name, email, phone, mobile_phone, cpf, cnpj, address, complement, city, state, zip_code_prefix, order_count) VALUES ";

        protected readonly string baseProductQuery = "INSERT INTO products (id, seller_id, name, sku, category_name, description, price, updated_at, active, status) VALUES ";

        protected readonly string baseStockQuery = "INSERT INTO stock_items (product_id, seller_id, qty_available, qty_reserved, order_count, ytd, data) VALUES ";

        protected readonly string baseCustomerQuery = "INSERT INTO customers (id, first_name, last_name, address, complement, birth_date, " +
                    "zip_code_prefix, city, state, " +
                    "card_number, card_security_number, card_expiration, card_holder_name, card_type, " +
                    "success_payment_count, failed_payment_count, delivery_count, abandoned_cart_count, data) VALUES ";

        protected void GenerateStockItem(DuckDbCommand command, int productId, int sellerId)
        {
            var quantity = numeric(4, true);
            var ytd = numeric(2, false);
            var order_count = numeric(2, false);
            var data = RandomString(50, alphanumeric);

            // issue insert statement
            var sb = new StringBuilder(baseStockQuery);
            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerId).Append(',');
            sb.Append(quantity).Append(',');
            sb.Append(0).Append(',');
            sb.Append(order_count).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append('\'').Append(data).Append("');");

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected Array cardValues = Enum.GetValues(typeof(CardBrand));

        protected void GenerateCustomer(DuckDbCommand command, int customerId, string[] geo)
        {
            var firstName = RandomString(16, alpha);
            var lastName = RandomString(16, alpha);
            var address = RandomString(20, alphanumeric);
            var complement = RandomString(20, alphanumeric);
            var birth_date = GenerateBirthdate();

            var city = geo[0];
            var state = geo[1];
            var zip = geo[2];

            var cardNumber = RandomString(16, numbers);
            var cardSecurityNumber = RandomString(3, numbers);
            var cardExpiration = RandomString(4, numbers);
            string cardHolderName;
            if (random.Next(1, 11) < 8)//70% change
            {
                var middleName = RandomString(16, alpha);
                cardHolderName = firstName + " " + middleName + " " + lastName;
            }
            else
            {
                cardHolderName = RandomString(16, alpha);
            }
            var cardType = cardValues.GetValue(random.Next(1, cardValues.Length)).ToString();

            var success_payment_count = numeric(2, false);
            var failed_payment_count = numeric(2, false);
            var C_DELIVERY_CNT = numeric(2, false);
            var C_DATA = RandomString(500, alphanumeric);

            var abandonedCartsNum = numeric(2, false);

            var sb = new StringBuilder(baseCustomerQuery);
            sb.Append('(').Append(customerId).Append(',');
            sb.Append('\'').Append(firstName).Append("',");
            sb.Append('\'').Append(lastName).Append("',");
            sb.Append('\'').Append(address).Append("',");
            sb.Append('\'').Append(complement).Append("',");
            sb.Append('\'').Append(birth_date).Append("',");
            sb.Append('\'').Append(zip).Append("',");
            sb.Append('\'').Append(city).Append("',");
            sb.Append('\'').Append(state).Append("',");
            sb.Append('\'').Append(cardNumber).Append("',");
            sb.Append('\'').Append(cardSecurityNumber).Append("',");
            sb.Append('\'').Append(cardExpiration).Append("',");
            sb.Append('\'').Append(cardHolderName).Append("',");
            sb.Append('\'').Append(cardType).Append("',");
            sb.Append(success_payment_count).Append(',');
            sb.Append(failed_payment_count).Append(',');
            sb.Append(C_DELIVERY_CNT).Append(',');
            sb.Append(abandonedCartsNum).Append(',');
            sb.Append('\'').Append(C_DATA).Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();

        }

        protected void GenerateSeller(DuckDbCommand command, long sellerId, string city, string state, string zip)
        {
            string name = RandomString(10, alpha);
            string company_name = RandomString(10, alpha);
            string email = GenerateRandomEmail();
            string phone = GeneratePhoneNumber();
            string mobile_phone = GeneratePhoneNumber();

            string cpf = GenerateCpf();
            string cnpj = GenerateCnpj();

            string address = RandomString(20, alphanumeric);
            string complement = RandomString(20, alphanumeric);

            var order_count = numeric(1, false);

            // issue insert statement
            var sb = new StringBuilder(baseSellerQuery);
            sb.Append('(').Append(sellerId).Append(',');
            sb.Append('\'').Append(name).Append("',");
            sb.Append('\'').Append(company_name).Append("',");
            sb.Append('\'').Append(email).Append("',");
            sb.Append('\'').Append(phone).Append("',");
            sb.Append('\'').Append(mobile_phone).Append("',");
            sb.Append('\'').Append(cpf).Append("',");
            sb.Append('\'').Append(cnpj).Append("',");
            sb.Append('\'').Append(address).Append("',");
            sb.Append('\'').Append(complement).Append("',");
            sb.Append('\'').Append(city).Append("',");
            sb.Append('\'').Append(state).Append("',");
            sb.Append('\'').Append(zip).Append("',");
            sb.Append(order_count).Append(");");
            
            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected void GenerateProduct(DuckDbCommand command, long productId, long sellerId, string category)
        {
            var name = RandomString(24, alphanumeric);
            // e.g., "PRDQQ1UCPOFRHWAA"
            var sku = RandomString(16, alphanumericupper);
            var price = numeric(5, 2, false);
            var description = RandomString(50, alphanumeric);
            var status = "approved";
            var sb = new StringBuilder(baseProductQuery);

            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerId).Append(',');
            sb.Append('\'').Append(name).Append("',");
            sb.Append('\'').Append(sku).Append("',");
            sb.Append('\'').Append(category).Append("',");
            sb.Append('\'').Append(description).Append("',");
            sb.Append(price).Append(',');
            sb.Append('\'').Append(DateTime.Now.ToLongDateString()).Append("',");
            sb.Append("TRUE").Append(',');
            sb.Append('\'').Append(status).Append("');");

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

        /**
         * Retrieved from:
         * http://www.java2s.com/example/csharp/system/generate-random-email.html
         */
        public static string GenerateRandomEmail()
        {
            return string.Format("{0}@{1}.com", GenerateRandomAlphabetString(10), GenerateRandomAlphabetString(10));
        }

        private static readonly Random seedRandom = new Random(Guid.NewGuid().GetHashCode());
        public static string GenerateRandomAlphabetString(int length)
        {
            char[] chars = new char[length];
            for (int i = 0; i < length; i++)
            {
                chars[i] = alpha[seedRandom.Next(alpha.Length)];
            }

            return new string(chars);
        }

        // Retrieved from: https://gist.github.com/georgepaoli/16fd769352646a888ebb157cfe982ff6
        private string GenerateCpf()
        {
            int soma = 0, resto = 0;
            int[] multiplicador1 = new int[9] { 10, 9, 8, 7, 6, 5, 4, 3, 2 };
            int[] multiplicador2 = new int[10] { 11, 10, 9, 8, 7, 6, 5, 4, 3, 2 };

            string semente = random.Next(100000000, 999999999).ToString();

            for (int i = 0; i < 9; i++)
                soma += int.Parse(semente[i].ToString()) * multiplicador1[i];

            resto = soma % 11;
            if (resto < 2)
                resto = 0;
            else
                resto = 11 - resto;

            semente = semente + resto;
            soma = 0;

            for (int i = 0; i < 10; i++)
                soma += int.Parse(semente[i].ToString()) * multiplicador2[i];

            resto = soma % 11;

            if (resto < 2)
                resto = 0;
            else
                resto = 11 - resto;

            semente = semente + resto;
            return semente;
        }

        private string GeneratePhoneNumber()
        {
            return this.random.Next(10000000, 99999999).ToString();
        }

        /**
         * 14-digit number. Format: XX.XXX.XXX/0001-XX
         * More info: https://en.wikipedia.org/wiki/CNPJ
         */
        private string GenerateCnpj()
        {
            string part1 = this.random.Next(10000000, 99999999).ToString();
            var part2 = this.random.Next(100000, 999999).ToString().ToCharArray();
            part2[3] = '1';
            return part1 + part2;
        }

        /**
         * Inspiration from: https://stackoverflow.com/questions/44370877/generate-a-random-birthday-given-an-age
         */
        private string GenerateBirthdate()
        {
            var toAdd = (long)(random.NextDouble() * baseTicks);
            var newDate = new DateTime(minDate.Ticks + toAdd);
            return newDate.ToLongTimeString();
        }

    }
}

