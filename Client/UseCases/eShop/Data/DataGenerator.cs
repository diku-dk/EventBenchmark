using System;
using System.Collections.Generic;
using System.Linq;
using Common.Entities.eShop;

namespace Client.Configuration
{
    public class DataGenerator
    {

        const string numbers = "0123456789";
        const string alphanumeric = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        public DataGenerator()
        {




        }

        private List<BasketItem> GenerateItems(int NumberOfItems)
        {
            List<BasketItem> items = new List<BasketItem>(NumberOfItems);

            for (int i = 0; i < NumberOfItems; i++)
            {

                BasketItem item = new BasketItem();

                item.Id = i.ToString();
                item.ProductId = i;
                item.ProductName = RandomString(8, alphanumeric);

                item.UnitPrice = (decimal)numeric(0, 100, false);
                item.OldUnitPrice = item.UnitPrice;
                item.Quantity = numeric(10, false);
                // item.PictureUrl = null;

                items.Add(item);

            }

            return items;
        }

        private List<ApplicationUser> GenerateCustomers(int NumberOfCustomers)
        {

            List<ApplicationUser> users = new List<ApplicationUser>(NumberOfCustomers);

            for (int i = 0; i < NumberOfCustomers; i++)
            {

                ApplicationUser user = new ApplicationUser();
                user.CardNumber = RandomString(8, numbers);
                user.SecurityNumber = RandomString(8, numbers);
                user.Expiration = "10/28";

                user.CardHolderName = RandomString(8, alphanumeric);
                user.CardType = numeric(2, false);

                user.Street = RandomString(8, alphanumeric);
                user.City = RandomString(8, alphanumeric);
                user.State = RandomString(8, alphanumeric);
                user.Country = RandomString(8, alphanumeric);
                user.ZipCode = RandomString(8, numbers);
                user.Name = RandomString(8, alphanumeric);
                user.LastName = RandomString(8, alphanumeric);

                users.Add(user);
            }


            return users;
        }


        private static string RandomString(int length, string chars)
        {
            var random = new Random();
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private static int numeric(int m, bool signed)
        {
            var random = new Random();
            var num = random.Next((int)Math.Pow(10, m), (int)Math.Pow(10, m + 1));
            var isPositive = random.Next(0, 2);
            if (signed && isPositive > 1) return -num;
            else return num;
        }

        private static float numeric(int m, int n, bool signed)
        {
            float the_number;
            var random = new Random();
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
