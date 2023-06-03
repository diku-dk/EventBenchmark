using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using System.Text;
using Confluent.Kafka;
using Bogus;
using Bogus.Extensions.Brazil;
using Common.Entity;

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

        protected readonly Faker faker = new Faker();

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
            var quantity = Numeric(4, true);
            var ytd = Numeric(2, false);
            var order_count = Numeric(2, false);
            var data = faker.Lorem.Sentence(); // RandomString(50, alphanumeric);

            // issue insert statement
            var sb = new StringBuilder(baseStockQuery);
            sb.Append('(').Append(productId).Append(',');
            sb.Append(sellerId).Append(',');
            sb.Append(quantity).Append(',');
            sb.Append(0).Append(',');
            sb.Append(order_count).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append('\'').Append(data).Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected Array cardValues = Enum.GetValues(typeof(CardBrand));

        protected void GenerateCustomer(DuckDbCommand command, int customerId, string[] geo)
        {
            var firstName = RemoveBadCharacter( faker.Name.FirstName() ); // RandomString(16, alpha);
            var lastName = RemoveBadCharacter( faker.Name.LastName() ); // RandomString(16, alpha);
            var address = RemoveBadCharacter(faker.Address.StreetName()); // RandomString(20, alphanumeric);
            var complement = RemoveBadCharacter(faker.Address.StreetSuffix()); // RandomString(20, alphanumeric);
            var birth_date = GenerateBirthdate();

            var city = geo[0];
            var state = geo[1];
            var zip = geo[2];

            var cardNumber = faker.Finance.CreditCardNumber(); //  RandomString(16, numbers);
            var cardSecurityNumber = faker.Finance.CreditCardCvv(); //RandomString(3, numbers);
            var cardExpiration = RandomString(4, numbers);
            string cardHolderName;
            if (random.Next(1, 11) < 8)//70% change
            {
                var middleName = RemoveBadCharacter( faker.Name.LastName() ); // RandomString(16, alpha);
                cardHolderName = firstName + " " + middleName + " " + lastName;
            }
            else
            {
                cardHolderName = RemoveBadCharacter(faker.Name.FullName()); // RandomString(16, alpha);
            }
            var cardType = cardValues.GetValue(random.Next(1, cardValues.Length)).ToString();

            var success_payment_count = Numeric(2, false);
            var failed_payment_count = Numeric(2, false);
            var C_DELIVERY_CNT = Numeric(2, false);
            var C_DATA = faker.Lorem.Paragraph(); // RandomString(500, alphanumeric);

            var abandonedCartsNum = Numeric(2, false);

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
            string name = RemoveBadCharacter(faker.Name.FullName()); // RandomString(10, alpha);
            string company_name = RemoveBadCharacter(faker.Company.CompanyName()); // RandomString(10, alpha);
            string email = GenerateRandomEmail();
            string phone = GeneratePhoneNumber();
            string mobile_phone = GeneratePhoneNumber();

            string cpf = faker.Person.Cpf(); // GenerateCpf();
            string cnpj = faker.Company.Cnpj(); // GenerateCnpj();

            var address = RemoveBadCharacter(faker.Address.StreetAddress()); // RandomString(20, alphanumeric);
            var complement = RemoveBadCharacter(faker.Address.SecondaryAddress()); // RandomString(20, alphanumeric);

            var order_count = Numeric(1, false);

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
            var name = faker.Commerce.ProductName(); // RandomString(24, alphanumeric);
            // e.g., "PRDQQ1UCPOFRHWAA"
            var sku = RandomString(16, alphanumericupper);
            var price = Numeric(5, 2, true); // decimal.Parse( faker.Commerce.Price() ); // 
            var description = RemoveBadCharacter( faker.Commerce.ProductDescription() );  // faker.Lorem.Paragraph(); // RandomString(50, alphanumeric);
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

        protected static string RemoveBadCharacter(string str)
        {
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

        protected int Numeric(int m, bool signed)
        {
            var num = random.Next((int)Math.Pow(10, m), (int)Math.Pow(10, m + 1));
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 1) return -num;
            else return num;
        }

        protected float Numeric(int m, int n, bool signed)
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

        public string GenerateRandomEmail()
        {
            return faker.Internet.Email();
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

        private string GeneratePhoneNumber()
        {
            return faker.Phone.PhoneNumber();
            // return this.random.Next(10000000, 99999999).ToString();
        }

        private string GenerateBirthdate()
        {
            return faker.Person.DateOfBirth.ToShortDateString();
        }

    }
}

