using Common.Entity;

namespace Marketplace.Message
{
	/*
	 * Currently not in use
	 */
	public class ProductCheck
	{
        public long Id { get; set; }
        public ItemStatus Status { get; set; }
		public decimal Price { get; set; }

		public ProductCheck(long Id)
		{
			this.Id = Id;
			this.Status = ItemStatus.IN_STOCK;
		}
    }
}

