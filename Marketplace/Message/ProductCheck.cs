using Common.Entity;

namespace Marketplace.Message
{
	/*
	 * Currently not in use
	 */
	public struct ProductCheck
	{
        public long Id { get; }
        public ItemStatus Status { get; }
		public decimal Price { get; }

		public ProductCheck(long id, ItemStatus status, decimal price )
		{
			this.Id = id;
			this.Status = status;
			this.Price = price;
		}
    }
}

