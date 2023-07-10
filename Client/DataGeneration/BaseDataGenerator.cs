using System;
using System.Collections.Generic;
using System.Linq;
using DuckDB.NET.Data;
using System.Text;
using Bogus;
using Bogus.Extensions.Brazil;
using Common.Entities;

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
            ["products"] = "CREATE OR REPLACE TABLE products (seller_id INTEGER, product_id INTEGER, name VARCHAR, sku VARCHAR, category VARCHAR, description VARCHAR, price REAL, freight_value REAL, created_at datetime, updated_at datetime, active BOOLEAN, status VARCHAR);",
            ["stock_items"] = "CREATE OR REPLACE TABLE stock_items (seller_id INTEGER, product_id INTEGER, qty_available INTEGER, qty_reserved INTEGER, order_count INTEGER, ytd INTEGER, data VARCHAR);",
            ["customers"] = "CREATE OR REPLACE TABLE customers (id INTEGER, first_name VARCHAR, last_name VARCHAR, address VARCHAR, complement VARCHAR, birth_date VARCHAR, " +
                            "zip_code VARCHAR, city VARCHAR, state VARCHAR, " +
                            "card_number VARCHAR, card_security_number VARCHAR, card_expiration VARCHAR, card_holder_name VARCHAR, card_type VARCHAR, " +
                            "success_payment_count INTEGER, failed_payment_count INTEGER, delivery_count INTEGER, abandoned_cart_count INTEGER, data VARCHAR);"
        };

        protected readonly string baseSellerQuery = "INSERT INTO sellers(id, name, company_name, email, phone, mobile_phone, cpf, cnpj, address, complement, city, state, zip_code_prefix, order_count) VALUES ";

        protected readonly string baseProductQuery = "INSERT INTO products (seller_id, product_id, name, sku, category, description, price, freight_value, active, status) VALUES ";

        protected readonly string baseStockQuery = "INSERT INTO stock_items (seller_id, product_id, qty_available, qty_reserved, order_count, ytd, data) VALUES ";

        protected readonly string baseCustomerQuery = "INSERT INTO customers (id, first_name, last_name, address, complement, birth_date, " +
                    "zip_code, city, state, " +
                    "card_number, card_security_number, card_expiration, card_holder_name, card_type, " +
                    "success_payment_count, failed_payment_count, delivery_count, abandoned_cart_count, data) VALUES ";

        public abstract void Generate(bool genCustomer = false);

        protected void GenerateStockItem(DuckDbCommand command, int productId, int sellerId)
        {
            int quantity = 10000;// Numeric(2, false);
            var ytd = Numeric(1, false);
            var data = faker.Lorem.Sentence();

            // issue insert statement
            var sb = new StringBuilder(baseStockQuery);
            sb.Append('(').Append(sellerId).Append(',');
            sb.Append(productId).Append(',');
            sb.Append(quantity).Append(',');
            sb.Append(0).Append(',');
            sb.Append(0).Append(',');
            sb.Append(ytd).Append(',');
            sb.Append('\'').Append(data).Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected Array cardValues = Enum.GetValues(typeof(CardBrand));

        private Geolocation Geolocation()
        {
            return new Geolocation(RemoveBadCharacter(faker.Address.City()), RemoveBadCharacter(faker.Address.State()), RemoveBadCharacter(faker.Address.ZipCode())); ;
        }

        protected void GenerateCustomer(DuckDbCommand command, int customerId)
        {
            GenerateCustomer(command, customerId, Geolocation());
        }

        protected void GenerateCustomer(DuckDbCommand command, int customerId, Geolocation geolocation)
        {
            var firstName = RemoveBadCharacter( faker.Name.FirstName() );
            var lastName = RemoveBadCharacter( faker.Name.LastName() );
            var address = RemoveBadCharacter(faker.Address.StreetName());
            var complement = RemoveBadCharacter(faker.Address.StreetSuffix());
            var birth_date = GenerateBirthdate();

            var cardNumber = faker.Finance.CreditCardNumber();
            var cardSecurityNumber = faker.Finance.CreditCardCvv();
            var cardExpiration = GenerateFutureDate();
            string expDate = cardExpiration.ToString("MMyy");
            string cardHolderName;
            if (random.Next(1, 11) < 8)//70% chance same person
            {
                var middleName = RemoveBadCharacter( faker.Name.LastName() );
                cardHolderName = firstName + " " + middleName + " " + lastName;
            }
            else
            {
                cardHolderName = RemoveBadCharacter(faker.Name.FullName());
            }
            var cardType = cardValues.GetValue(random.Next(1, cardValues.Length)).ToString();

            var C_DATA = RemoveBadCharacter(faker.Lorem.Sentence());

            var sb = new StringBuilder(baseCustomerQuery);
            sb.Append('(').Append(customerId).Append(',');
            sb.Append('\'').Append(firstName).Append("',");
            sb.Append('\'').Append(lastName).Append("',");
            sb.Append('\'').Append(address).Append("',");
            sb.Append('\'').Append(complement).Append("',");
            sb.Append('\'').Append(birth_date).Append("',");
            sb.Append('\'').Append(geolocation.zipcode).Append("',");
            sb.Append('\'').Append(geolocation.city).Append("',");
            sb.Append('\'').Append(geolocation.state).Append("',");
            sb.Append('\'').Append(cardNumber).Append("',");
            sb.Append('\'').Append(cardSecurityNumber).Append("',");
            sb.Append('\'').Append(expDate).Append("',");
            sb.Append('\'').Append(cardHolderName).Append("',");
            sb.Append('\'').Append(cardType).Append("',");
            sb.Append(0).Append(','); // success_payment_count
            sb.Append(0).Append(','); // failed_payment_count
            sb.Append(0).Append(','); // delivery_count
            sb.Append(0).Append(','); // abandoned carts
            sb.Append('\'').Append(C_DATA).Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();

        }

        protected void GenerateSeller(DuckDbCommand command, long sellerId)
        {
            GenerateSeller(command, sellerId, Geolocation());
        }

        protected void GenerateSeller(DuckDbCommand command, long sellerId, Geolocation geolocation)
        {
            string name = RemoveBadCharacter(faker.Name.FullName());
            string company_name = RemoveBadCharacter(faker.Company.CompanyName());
            string email = GenerateRandomEmail();
            string phone = GeneratePhoneNumber();
            string mobile_phone = GeneratePhoneNumber();

            string cpf = faker.Person.Cpf();
            string cnpj = faker.Company.Cnpj();

            var address = RemoveBadCharacter(faker.Address.StreetAddress());
            var complement = RemoveBadCharacter(faker.Address.SecondaryAddress());

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
            sb.Append('\'').Append(geolocation.city).Append("',");
            sb.Append('\'').Append(geolocation.state).Append("',");
            sb.Append('\'').Append(geolocation.zipcode).Append("',");
            sb.Append(0).Append(");");
            
            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

        protected void GenerateProduct(DuckDbCommand command, long productId, long sellerId)
        {
            this.GenerateProduct(command, productId, sellerId, faker.Commerce.Categories(1)[0]);
        }

        protected void GenerateProduct(DuckDbCommand command, long productId, long sellerId, string category)
        {
            var name = faker.Commerce.ProductName();
            // e.g., "PRDQQ1UCPOFRHWAA"
            var sku = RandomString(16, alphanumericupper);
            var price = Numeric(4, 2, false);
            var freight_value = Numeric(3, 2, false);
            var description = RemoveBadCharacter( faker.Commerce.ProductDescription() );
            var sb = new StringBuilder(baseProductQuery);

            DateTime now = DateTime.Now;

            sb.Append('(').Append(sellerId).Append(',');
            sb.Append(productId).Append(',');
            sb.Append('\'').Append(name).Append("',");
            sb.Append('\'').Append(sku).Append("',");
            sb.Append('\'').Append(category).Append("',");
            sb.Append('\'').Append(description).Append("',");
            sb.Append(price).Append(',');
            sb.Append(freight_value).Append(',');
            sb.Append("TRUE").Append(',');
            sb.Append('\'').Append("approved").Append("');");

            Console.WriteLine(sb.ToString());

            command.CommandText = sb.ToString();
            command.ExecuteNonQuery();
        }

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
        }

        private string GenerateBirthdate()
        {
            return faker.Person.DateOfBirth.ToShortDateString();
        }

        private DateTime GenerateFutureDate()
        {
            return DateTime.Today.AddMilliseconds(random.Next(1, int.MaxValue));
        }

    }
}

