using System;
using Marketplace.Entity;
using System.Threading.Tasks;
using Orleans;
using System.Collections.Generic;

namespace Marketplace.Actor
{
    public interface IStockActor : IGrainWithIntegerKey
    {
        public Task DeleteItem(long productId);
        public Task<ItemStatus> AttemptReservation(long productId, int quantity);
        public Task CancelReservation(long productId, int quantity);
        public Task ConfirmReservation(long productId, int quantity);
        public Task ConfirmOrder(long productId, int quantity);

        // API
        public Task AddItem(StockItem item);
    }


    public class StockActor : Grain, IStockActor
	{

        private Dictionary<long, StockItem> items;

		public StockActor()
		{
            this.items = new();
        }

        public Task DeleteItem(long productId)
        {
            return Task.FromResult(items.Remove(productId));
        }

        // called by order actor only
        public Task<ItemStatus> AttemptReservation(long productId, int quantity)
        {
            if (!items.ContainsKey(productId))
            {
                return Task.FromResult(ItemStatus.DELETED);

            }
            if (items[productId].qty_available - items[productId].qty_reserved >= quantity)
            {
                items[productId].qty_reserved += quantity;
                return Task.FromResult(ItemStatus.IN_STOCK);
            }

            return Task.FromResult(ItemStatus.OUT_OF_STOCK);
        }

        // called by order actor only
        // deduct from stock reservation
        public Task ConfirmReservation(long productId, int quantity)
        {
            // deduct from stock
            items[productId].qty_available -= quantity;
            items[productId].qty_reserved -= quantity;
            return Task.CompletedTask;
        }

        // called by payment and order actors only
        public Task CancelReservation(long productId, int quantity)
        {
            // return item to stock
            items[productId].qty_reserved -= quantity;
            return Task.CompletedTask;
        }

        // called by payment actor only.
        // deduct from stock available
        public Task ConfirmOrder(long productId, int quantity)
        {
            // increase order count
            items[productId].order_count += 1;
            return Task.CompletedTask;
        }

        public Task AddItem(StockItem item)
        {
            return Task.FromResult(items.TryAdd(item.product_id, item));
        }
    }
}

