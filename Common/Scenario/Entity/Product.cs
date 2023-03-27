using System;
namespace Common.Scenario.Entity
{
	public class Product
	{
        public long Id { get; set; }

        public long SellerId { get; set; }

        public string Name { get; set; }

        public string Category { get; set; }

        public string Data { get; set; }

        public decimal Price { get; set; }
    }
}

